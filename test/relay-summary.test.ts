import test from 'node:test';
import assert from 'node:assert/strict';
import { onlineSummary, relayState, relayTone } from '../src/relay-summary.ts';
import type { RelayStatus } from '../src/types.ts';

const relay = (url: string, extra: Partial<RelayStatus> = {}): RelayStatus => ({
  url,
  phase: 'finished',
  connection: 'disconnected',
  imported: 0,
  sent: 0,
  present: 0,
  failed: 0,
  pending: 0,
  ...extra,
});

test('idle relays are up to date, whatever their closed connection says', () => {
  const relays = [relay('wss://a.example'), relay('wss://b.example', { phase: 'idle' })];
  assert.deepEqual(onlineSummary(relays, 0, false), { tone: 'ok', text: 'Online copies are up to date on all 2 relays.' });
  assert.equal(relayState(relays[1]), 'Up to date');
  assert.equal(relayTone(relays[1]), 'ok');
  assert.equal(onlineSummary([relay('wss://a.example/')], 0, false).text, 'Online copies are up to date on a.example.');
});

test('a failing relay is named with its reason and the items still waiting for it', () => {
  const relays = [
    relay('wss://a.example'),
    relay('wss://nos.example', { phase: 'error', detail: 'connection timed out', pending: 67 }),
  ];
  assert.deepEqual(onlineSummary(relays, 67, false), {
    tone: 'warn',
    text: 'Online copies are up to date on 1 of 2 relays.',
    problem: 'Sync with nos.example failed: connection timed out. Social Backup keeps trying to send 67 items.',
  });
  assert.equal(relayState(relays[1]), 'Failed');
  const two = [...relays, relay('wss://c.example', { phase: 'error', detail: 'connection failed' })];
  assert.equal(
    onlineSummary(two, 67, false).problem,
    'Sync failed with nos.example and c.example. Social Backup keeps trying to send 67 items.',
  );
});

test('every relay failing keeps the reassurance that the backup is local', () => {
  const down = [relay('wss://a.example', { phase: 'error' }), relay('wss://b.example', { phase: 'error' })];
  assert.deepEqual(onlineSummary(down, 1, false), {
    tone: 'warn',
    text: 'Sync failed with every relay. Your backup is safe on this device.',
    problem: 'Social Backup keeps trying to send 1 item.',
  });
});

test('an older stored timestamp refusal is distinguished from a relay cooldown', () => {
  const tooOld = relay('wss://a.example', { phase: 'retry', detail: 'invalid: created_at too early', pending: 245 });
  assert.equal(relayState(tooOld), 'Event too old for relay');
  assert.match(onlineSummary([tooOld], 245, false).text, /rejected an event because of its original date/);
  assert.equal(relayState({ ...tooOld, detail: 'network timeout' }), 'Will retry');
  assert.equal(relayState({ ...tooOld, retryAt: Date.now() + 60000 }), 'Asked to wait');
});

test('waiting items, a running sync and no relays each say so', () => {
  assert.deepEqual(onlineSummary([relay('wss://a.example', { pending: 2, phase: 'idle' }), relay('wss://b.example')], 2, false), {
    tone: 'busy',
    text: 'Online copies are up to date on 1 of 2 relays. 2 items waiting to send.',
    problem: undefined,
  });
  assert.equal(onlineSummary([relay('wss://a.example', { phase: 'broadcasting', pending: 3 })], 3, true).text, 'Syncing… 3 items left to send.');
  assert.equal(onlineSummary([], 0, false).tone, 'off');
});
