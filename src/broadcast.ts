import { errorMessage, type Archive, type Delivery, type RelayProgress, type Settings } from './types.ts';
import type { ImportResult } from './relays.ts';
import { normalizeURL } from 'nostr-tools/utils';
import { discardHistoryBody } from './event-history.ts';
import { PUBLIC_POST_KINDS } from './model.ts';
import { pool, publish } from './relays.ts';
import { compareEvents } from './reconcile.ts';
import { canDeliverCapture } from './social.ts';
import { drainRelayOutbox, RelayRateLimitError } from 'nostr-pubsub';
import { rejectedForAge } from './relay-summary.ts';

export async function deliverPending(
  db: Archive,
  url: string,
  rows: Delivery[],
  settings: () => Promise<Settings>,
  pubkey: string,
  imported?: ImportResult,
  progress?: RelayProgress,
) {
  // A kind-limited import cannot establish presence of the entire broadcast queue.
  const have = imported?.kinds ? undefined : imported?.have;
  const previous = { ...progress };
  let mode = have ? 'Negentropy broadcast' : imported && !imported.have ? 'Direct broadcast' : undefined;
  const { sent, present, failed, error } = await drainRelayOutbox(url, rows, {
    async enabled(row) {
      const conf = await settings();
      return (
        conf.pubkey === pubkey && conf.relays.includes(url) && canDeliverCapture(row, conf) &&
        (!('historySlot' in row) || !!await db.getKey('outbox', [row.id, url])) &&
        !(await db.getKey('heldEvents', row.id))
      );
    },
    async send(row) {
      const event = await db.get('events', row.id);
      if (!event) {
        if ('historySlot' in row && !await db.getKey('outbox', [row.id, url])) return;
        throw Error('Queued event is missing from the archive.');
      }
      try {
        await publish(url, event);
      } catch (error) {
        if (progress && rejectedForAge(errorMessage(error))) {
          const prior = progress.ageRejections;
          progress.ageRejections = { count: (prior?.count ?? 0) + 1, oldest: Math.min(prior?.oldest ?? event.created_at, event.created_at) };
        }
        throw error;
      }
    },
    async acknowledge(ids) {
      const tx = db.transaction(['outbox', 'events', 'meta', 'mediaRefs', 'heldEvents'], 'readwrite');
      const expired = await tx.objectStore('meta').count(IDBKeyRange.bound(['expiredHistory'], ['expiredHistory', []]));
      for (const id of ids) {
        tx.objectStore('outbox').delete([id, url]);
        if (expired && await tx.objectStore('meta').get(['expiredHistory', id])) await discardHistoryBody(tx, id);
      }
      await tx.done;
    },
    connected: () => !!pool.listConnectionStatus().get(normalizeURL(url)),
    onProgress(state) {
      if (!progress) return;
      progress.sent = (previous.sent ?? 0) + state.sent;
      progress.present = (previous.present ?? 0) + state.present;
      progress.failed = (previous.failed ?? 0) + state.failed;
      if (state.error) progress.detail = state.error;
    },
    async missing(batch) {
      if (have && imported) {
        return new Set(
          batch
            .filter(
              (row) =>
                row.created_at === undefined ||
                row.created_at < (imported.since ?? 0) ||
                row.created_at > (imported.until ?? Infinity) ||
                have.has(row.id),
            )
            .map((row) => row.id),
        );
      }
      if (mode === 'Direct broadcast') return;
      const since = batch[0].created_at,
        until = batch[batch.length - 1].created_at;
      if (since === undefined || until === undefined) return;
      try {
        const inventory = batch.map((row) => ({ id: row.id, created_at: row.created_at! }));
        const result = await compareEvents(
          url,
          { authors: [pubkey], kinds: PUBLIC_POST_KINDS, since, until },
          inventory,
          false,
        );
        mode = 'Negentropy broadcast';
        return result.have;
      } catch (error) {
        if (error instanceof RelayRateLimitError) throw error;
        mode = 'Direct broadcast';
      }
    },
  });
  const report = [];
  if (mode && rows.some((row) => row.broadcast)) report.push(mode);
  if (sent) report.push(`${sent} acknowledged`);
  if (present) report.push(`${present} already on relay`);
  if (failed) report.push(`${failed} pending retry (${error})`);
  return report.join(' · ');
}
