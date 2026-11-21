<script lang="ts">
  import type { RelayStatus } from '../types.ts';
  import { ageRejectionDetail, rejectedForAge, relayHost, relayState, relayTone, syncedAt } from '../relay-summary.ts';

  let { relays }: { relays: RelayStatus[] } = $props();

  // Relays close their connections after each sync, so only a live connection is worth showing.
  const connection = { connected: 'Connected', connecting: 'Connecting…', disconnected: '' };
  const detail = (relay: RelayStatus) =>
    relay.retryAt
      ? 'Retry after ' + new Date(relay.retryAt).toLocaleTimeString([], { timeStyle: 'short' })
      : relay.detail && !rejectedForAge(relay.detail) && ['retry', 'error', 'partial'].includes(relay.phase)
        ? relay.detail
        : '';
</script>

<ul id="reports" class="relayRows" aria-label="Relay status">
  {#each relays as relay (relay.url)}
    {@const total = relay.sent + relay.present + relay.pending}
    {@const ageDetail = ageRejectionDetail(relay)}
    <li class="relayRow {relayTone(relay)}">
      <div>
        <strong title={relay.url}>{relayHost(relay.url)}</strong>
        {#if connection[relay.connection]}<small class="relayConnection">{connection[relay.connection]}</small>{/if}
      </div>
      <div>
        <span class="relayState">{relayState(relay)}</span>
        {#if detail(relay)}<small>{detail(relay)}</small>{/if}
        {#if ageDetail}<small>{ageDetail}</small>{/if}
        {#if relay.lastSyncedAt}<small>Last download check {syncedAt(relay.lastSyncedAt)}</small>{/if}
      </div>
      <div class="relayCounts">
        {#if relay.imported}<small>{relay.imported.toLocaleString()} downloaded</small>{/if}
        {#if total}
          <small>{relay.sent.toLocaleString()} sent · {relay.pending.toLocaleString()} queued{ageDetail ? ' in total' : ''}</small>
          {#if relay.present}<small>{relay.present.toLocaleString()} already on relay</small>{/if}
          {#if relay.pending || relay.phase === 'broadcasting'}
            <progress
              max={total}
              value={relay.sent + relay.present}
              aria-label={'Sending progress for ' + relayHost(relay.url)}
            ></progress>
          {/if}
        {/if}
      </div>
    </li>
  {/each}
</ul>
