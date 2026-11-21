import { signedInvoice, signedZap } from './tip-fixtures.ts';
import type { Filter } from 'nostr-tools/filter';
import type { Event as NostrEvent } from 'nostr-tools/pure';
import type { Action, Result, Reply, Message } from '../src/types.ts';
import type { Sender } from '../src/platform.d.ts';
import type { TestContext } from 'node:test';
import 'fake-indexeddb/auto';
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { runInThisContext } from 'node:vm';
import { setTimeout as delay } from 'node:timers/promises';
import {
  generateSecretKey,
  finalizeEvent,
  getPublicKey,
  verifyEvent,
} from 'nostr-tools/pure';
import { nsecEncode, npubEncode, noteEncode, decode } from 'nostr-tools/nip19';
import { NostrEventStore } from '@hashtree/nostr';
import { hex } from '../src/model.ts';
import { permissionDecision } from '../src/signer-storage.ts';
import { readOnlyStore } from '../src/blocks.ts';
import { append, openArchive, readRoot, stagePosts } from '../src/archive.ts';
import { Reconciliation } from 'nostr-pubsub-reconcile';
import { relayRetryDeadline as retryDeadline } from 'nostr-pubsub';
import { build } from 'esbuild';
import { svelte } from '../scripts/svelte.ts';
import { Window } from 'happy-dom';
import { publicProfileHTML, publicTweetHTML } from './fixtures.ts';
import { DEFAULT_RELAYS } from '../src/default-relays.ts';
import { syncedAt } from '../src/relay-summary.ts';
import { DEFAULT_BLOSSOM_SERVER } from '../src/media-refs.ts';
import { createHash } from 'node:crypto';

const key = generateSecretKey(),
  pubkey = getPublicKey(key);
const native = finalizeEvent(
  { kind: 1, created_at: 2, tags: [], content: 'Existing relay note' },
  key,
);
let accept = false;
const transmissions: NostrEvent[] = [],
  subscriptions: Filter[] = [];
interface FixtureRelay {
  supported: boolean;
  sent: NostrEvent[];
  stored: Map<string, NostrEvent>;
  filters: Filter[];
  reads: Filter[];
  withheld?: string;
  reject?: string;
  rejectBefore?: number;
  negLimit?: string[];
  readLimit?: string[];
  publishLimit?: string[];
  holdPublish?: Promise<void>;
  holdRead?: Promise<void>;
  /** A relay that never answers the connection, or refuses it. */
  connect?: 'hang' | 'refuse';
}
const broadcastRelays = new Map<string, FixtureRelay>();
const wireOpens: string[] = [];
class Wire extends EventTarget {
  static OPEN = 1;
  destination?: FixtureRelay;
  readyState = 1;
  neg?: Reconciliation;
  onopen?: () => void;
  onerror?: () => void;
  onmessage?: (message: MessageEvent<string>) => void;
  onclose?: (event: { code: number; reason: string }) => void;
  constructor(url: string) {
    super();
    wireOpens.push(new URL(url).href);
    this.destination = broadcastRelays.get(new URL(url).href);
    if (this.destination?.connect === 'hang') this.readyState = 0;
    else if (this.destination?.connect === 'refuse') {
      this.readyState = 3;
      queueMicrotask(() => this.onerror?.());
    } else queueMicrotask(() => this.onopen?.());
  }
  async send(frame: string) {
    const [type, id, filter, opening] = JSON.parse(frame);
    const receive = (data: unknown[]) =>
      setTimeout(() => {
        const message = new MessageEvent('message', { data: JSON.stringify(data) });
        this.dispatchEvent(message);
        this.onmessage?.(message);
      }, 0);
    const dest = this.destination;
    if (type === 'NEG-OPEN' || type === 'REQ') await dest?.holdRead;
    if (type === 'NEG-OPEN' || type === 'NEG-MSG') {
      if (type === 'NEG-OPEN') {
        dest?.filters.push(filter);
        if (dest?.negLimit) return receive(['NEG-ERR', id, ...dest.negLimit]);
        if (!dest?.supported) return receive(['NEG-ERR', id, 'unsupported']);
        const records = [...dest.stored.values()]
          .filter((e) => filter.authors.includes(e.pubkey) && (!filter.kinds || filter.kinds.includes(e.kind)))
          .map((e) => ({
            id: Buffer.from(e.id, 'hex'),
            timestamp: BigInt(e.created_at),
          }));
        this.neg = new Reconciliation(records, {
          since: BigInt(filter.since),
          until: BigInt(filter.until),
        });
      }
      assert.ok(this.neg);
      const reply = await this.neg.respond(Buffer.from(opening ?? filter, 'hex'));
      receive(['NEG-MSG', id, Buffer.from(reply).toString('hex')]);
    }
    if (type === 'REQ') {
      subscriptions.push(filter);
      dest?.reads?.push(filter);
      if (dest?.readLimit) return receive(['CLOSED', id, ...dest.readLimit]);
      // Tag filters such as #i match any listed value, as on real relays.
      const tagsMatch = (e: NostrEvent) => Object.entries(filter).every(([key, values]) =>
        !key.startsWith('#') || e.tags.some(([name, value]) => name === key.slice(1) && (values as string[]).includes(value)));
      const matches = (dest ? [...dest.stored.values()] : [native]).filter(
        (e) =>
          e.id !== dest?.withheld &&
          e.created_at <= (filter.until ?? Infinity) &&
          e.created_at >= (filter.since ?? 0) &&
          (!filter.ids || filter.ids.includes(e.id)) &&
          (!filter.authors || filter.authors.includes(e.pubkey)) &&
          (!filter.kinds || filter.kinds.includes(e.kind)) &&
          tagsMatch(e),
      );
      matches.sort((a, b) => b.created_at - a.created_at);
      for (const e of matches.slice(0, filter.limit)) receive(['EVENT', id, e]);
      receive(['EOSE', id]);
    }
    if (type === 'EVENT') {
      if (!dest) transmissions.push(id);
      const ok = dest ? dest.reject !== id.id : accept;
      if (dest) {
        dest.sent.push(id);
        await dest.holdPublish;
        if (dest.publishLimit) return receive(['OK', id.id, false, ...dest.publishLimit]);
        if (dest.rejectBefore && id.created_at < dest.rejectBefore)
          return receive(['OK', id.id, false, 'invalid: created_at too early']);
        if (ok) dest.stored.set(id.id, id);
      }
      receive(['OK', id.id, ok, ok ? '' : 'blocked: temporary test failure']);
    }
  }
  close() {
    this.readyState = 3;
    this.onclose?.({ code: 1000, reason: 'closed' });
  }
}
function listener<F = () => void>() {
  const list: F[] = [];
  return { addListener: (f: F) => list.push(f), list };
}
function area(onSet?: (values: Record<string, unknown>) => void) {
  const state: Record<string, unknown> = {};
  return {
    async get(k: string | null) {
      if (k === null) return structuredClone(state);
      return { [k]: structuredClone(state[k]) };
    },
    async set(v: Record<string, unknown>) {
      Object.assign(state, structuredClone(v));
      onSet?.(v);
    },
    async clear() {
      for (const k of Object.keys(state)) delete state[k];
    },
    async remove(k: string) {
      delete state[k];
    },
  };
}
const extensionId = 'test-extension',
  ext = 'chrome-extension://' + extensionId + '/';
let winURL: string | undefined;
// Tabs the extension opened.
const tabURLs: string[] = [];
// The badge and tooltip the extension last set on each tab's toolbar icon.
const badges = new Map<number, { path?: Record<number, string>; title?: string }>();
const badge = (tabId: number) => badges.get(tabId) ?? badges.set(tabId, {}).get(tabId)!;
function approvalScreen() {
  assert.ok(winURL);
  return { ...ui, url: winURL };
}
type Injection = {
  world?: 'MAIN';
  target: { tabId: number };
  func: Function;
  args?: unknown[];
};
const storageChanged = listener<(changes: object, area: string) => void>();
let popupPath: string | undefined;
const api = {
  action: {
    onClicked: listener(),
    setPopup: async ({ popup }: { popup: string }) => {
      popupPath = popup;
    },
    setIcon: async ({ path, tabId }: { path: Record<number, string>; tabId: number }) => void (badge(tabId).path = path),
    setTitle: async ({ title, tabId }: { title: string; tabId: number }) => void (badge(tabId).title = title),
  },
  scripting: {
    executeScript: async (_: Injection): Promise<{ result: unknown }[]> => {
      throw Error('Unexpected script injection');
    },
  },
  storage: {
    onChanged: storageChanged,
    local: area((values) => {
      if ('settingsChanged' in values)
        for (const f of storageChanged.list)
          f({ settingsChanged: { newValue: values.settingsChanged } }, 'local');
    }),
    session: area(),
  },
  runtime: {
    id: extensionId,
    openOptionsPage: async () => {},
    getURL: (p: string) => ext + p,
    onMessage:
      listener<
        (
          message: object,
          sender: Sender,
          reply: (response: Reply<unknown>) => void,
        ) => boolean
      >(),
    onInstalled: listener(),
    onStartup: listener(),
  },
  alarms: {
    get: async () => true,
    create: async () => {},
    onAlarm: listener<(alarm: { name: string }) => void>(),
  },
  tabs: {
    query: async () => [] as { id: number; url: string }[],
    onRemoved: listener<(id: number) => void>(),
    get: async () => ({ url: 'https://client.example/' }),
    create: async (o: { url: string }) => {
      tabURLs.push(o.url);
      return {};
    },
  },
  windows: {
    onRemoved: listener(),
    create: async (o: { url: string }) => {
      winURL = o.url;
      return { id: 1 };
    },
  },
};
Object.defineProperty(globalThis, 'chrome', { configurable: true, value: api });
Object.defineProperty(globalThis, 'WebSocket', { configurable: true, value: Wire });
// Node 22 lacks Web Locks. This fixture issues worker deletion requests sequentially.
if (!navigator.locks)
  Object.defineProperty(navigator, 'locks', {
    configurable: true,
    value: { request: async (_name: string, callback: () => unknown) => callback() },
  });
const buildResult = await build({
  entryPoints: ['src/background.ts'],
  bundle: true,
  minify: true,
  format: 'iife',
  write: false,
  target: 'chrome120',
});
runInThisContext(buildResult.outputFiles[0].text);
const ui = { id: extensionId, url: ext + 'archive.html', frameId: 0 };
const x = {
  id: extensionId,
  url: 'https://x.com/alice',
  tab: { id: 1, url: 'https://x.com/alice' },
  frameId: 0,
};
function publicTweet(input: RequestInfo | URL, caption: string, time = 1700000000) {
  const url = new URL(String(input));
  assert.equal(url.origin, 'https://x.com');
  const [, author, , id] = url.pathname.split('/');
  const response = new Response(publicTweetHTML(author, id, caption, time), { headers: { 'Content-Type': 'text/html' } });
  Object.defineProperty(response, 'url', { value: url.href });
  return response;
}
function call<K extends Action>(
  type: K,
  data: object = {},
  sender: Sender = ui,
): Promise<Reply<Result<K>>> {
  return new Promise((resolve) =>
    api.runtime.onMessage.list[0]({ type, ...data }, sender, (response) =>
      resolve(response as Reply<Result<K>>),
    ),
  );
}
async function success<K extends Action>(
  type: K,
  data: object = {},
  sender: Sender = ui,
): Promise<Result<K>> {
  const response = await call(type, data, sender);
  if (!response.ok) assert.fail(response.error);
  return response.value;
}
async function until(fn: () => unknown) {
  for (let i = 0; i < 200; i++) {
    if (await fn()) return;
    await delay(20);
  }
  assert.fail('timed out');
}

test('the toolbar opens welcome when signed out and shows the backup popup for an active profile', async (t) => {
  const options = t.mock.method(api.runtime, 'openOptionsPage');
  await until(() => popupPath === '');
  api.action.onClicked.list[0]();
  assert.equal(options.mock.callCount(), 1);
  const page = await extensionPage(
    t,
    'archive',
    installPageStorage,
    ext + 'archive.html#media',
  );
  const id = pageElements(page);
  // Input and change events bubble in browsers. Svelte listens for them at the page root.
  const dispatch = (name: string, event: string) =>
    page.document.getElementById(name)!.dispatchEvent(new page.Event(event, { bubbles: true }));
  await until(() => !id('onboarding').hidden);
  assert.equal(id('workspace').hidden, true);
  // The first screen asks whether you are new or already on Nostr.
  assert.equal(page.document.activeElement?.id, 'startSignUp');
  id('existingProfile').click();
  assert.equal(page.document.querySelector('#keyChoice'), null);
  assert.equal(page.document.querySelector('#loginForm button'), null);
  id('loginKey').value = 'invalid';
  dispatch('loginKey', 'input');
  assert.equal(id('notice').textContent, '');
  id('loginForm').requestSubmit();
  await until(() => id('notice').textContent === 'Check your secret key or user ID and try again.');
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  const profile = finalizeEvent(
    {
      kind: 0,
      created_at: 1,
      tags: [],
      content: JSON.stringify({
        name: 'alice',
        display_name: 'Alice Example',
        picture: 'https://images.example/alice.png',
      }),
    },
    secret,
  );
  const archive = await openArchive('nostr-backup-' + owner);
  await append(archive, [{ event: profile }], owner);
  archive.close();
  id('loginKey').value = npubEncode(owner).slice(0, -1) + '!';
  dispatch('loginKey', 'input');
  await until(() => id('notice').textContent === 'Check your secret key or user ID and try again.');
  assert.equal((await success('status')).conf.pubkey, '');
  id('loginKey').value = '  ' + npubEncode(owner) + '\n';
  dispatch('loginKey', 'input');
  dispatch('loginKey', 'change');
  // A user ID only gives read-only mode, so it asks first.
  await until(() => !!page.document.getElementById('watchConfirm'));
  assert.match(id('readOnlyWarning').textContent, /can only keep a copy/);
  assert.equal(id('notice').textContent, '');
  id('watchConfirm').click();
  await until(() => !id('workspace').hidden);
  assert.equal(id('onboarding').hidden, true);
  assert.equal(popupPath, 'popup.html');
  assert.equal((await success('status')).unlocked, false);
  assert.deepEqual((await success('status')).conf.relays, DEFAULT_RELAYS);
  assert.equal((await success('status')).conf.relaySync, true);
  assert.equal(id('relaySummary').textContent, `Choose relays (${DEFAULT_RELAYS.length} selected)`);
  assert.equal(page.document.querySelector('nav a[href="#settings"]'), null);
  const irisProfile = 'https://iris.to/' + npubEncode(owner);
  assert.equal(id('signedInProfile').getAttribute('href'), irisProfile);
  assert.equal(id('signedInProfile').getAttribute('target'), '_blank');
  assert.equal(id('displayName').textContent, 'Alice Example');
  assert.equal(id('profileIdentity').textContent, '@alice');
  // The Iris avatar loads the picture directly and falls back to an identicon if it fails.
  const avatar = () => page.document.querySelector('#signedInProfile img.iris-avatar-image');
  await until(() => avatar()?.getAttribute('src') === 'https://images.example/alice.png');
  avatar()!.dispatchEvent(new page.Event('error'));
  await until(() => !avatar() && !!page.document.querySelector('#signedInProfile .iris-minidenticon'));
  assert.equal(id('profileMode').hidden, false);
  assert.equal(id('capture').disabled, true);
  assert.equal(id('sourcesReadOnly').hidden, false);
  assert.equal(id('signedInProfile').textContent.includes('npub'), false);
  await until(async () => !(await success('status')).running);
  page.document.querySelector('#relayChoices')!.querySelector('input')!.click();
  await until(
    async () => (await success('status')).conf.relays.length === DEFAULT_RELAYS.length - 1,
  );
  await until(() => id('notice').textContent === 'Saved.');
  assert.equal(id('relaySummary').textContent, `Choose relays (${DEFAULT_RELAYS.length - 1} selected)`);
  id('customRelay').value = 'custom.example';
  id('addRelay').click();
  await until(async () =>
    (await success('status')).conf.relays.includes('wss://custom.example/'),
  );
  const popup = await extensionPage(t, 'popup');
  await until(() => !pageElements(popup)('backupActions').hidden);
  assert.equal(pageElements(popup)('open').textContent, 'Open backup');
  assert.equal(popup.document.getElementById('signer'), null);
  // In the popup, the profile header opens the backup page.
  assert.equal(popup.document.querySelector('#displayName')?.closest('a')?.getAttribute('href'), 'archive.html');
  assert.equal(pageElements(popup)('displayName').textContent, 'Alice Example');
  assert.equal(pageElements(popup)('profileMode').hidden, false);
  // Away from X, Facebook and Instagram the popup has no site line, only the size of the backup.
  assert.equal(pageElements(popup)('siteState').hidden, true);
  assert.match(pageElements(popup)('summary').textContent!, /^\d+ items? in your backup$/);
  await openLogoutConfirmation(page);
  assert.equal((await success('status')).conf.pubkey, owner);
  id('cancelLogout').click();
  await until(() => !page.document.getElementById('confirmLogout'));
  assert.equal((await success('status')).conf.pubkey, owner);
  assert.equal(id('workspace').hidden, false);
  id('logout').click();
  await until(() => !!page.document.getElementById('confirmLogout'));
  id('confirmLogout').click();
  await until(() => !id('onboarding').hidden);
  assert.equal(popupPath, '');
  assert.equal(id('welcome').hidden, false);
  assert.equal(id('nameForm').hidden, true);
  id('startSignUp').click();
  await until(() => !id('accountsForm').hidden);
  assert.equal(id('profileName').value, '');
  assert.equal(id('signedInProfile').hidden, true);
  assert.equal(id('signedInProfile').hasAttribute('href'), false);
  // Skipping the accounts leaves the name to type. A pasted secret key signs in instead.
  id('skipAccounts').click();
  await until(() => !id('nameForm').hidden);
  id('profileName').value = nsecEncode(secret);
  dispatch('profileName', 'input');
  assert.equal(id('profileName').value, '');
  // No accounts were chosen, so the backup opens without another setup screen.
  await until(() => !id('workspace').hidden);
  assert.equal((await success('status')).unlocked, true);
  assert.equal(id('profileMode').hidden, true);
  assert.equal(id('capture').disabled, false);
  assert.equal(id('loginKey').value, '');
  id('lock').click();
  await until(() => !id('unlock').hidden);
  assert.equal(page.document.getElementById('copyPrivateKey'), null);
  assert.equal(id('copyProfileId').hidden, false);
  id('secret').value = nsecEncode(secret);
  dispatch('secret', 'input');
  await until(() => id('unlock').hidden);
  assert.equal(id('secret').value, '');
  assert.equal((await success('status')).unlocked, true);
  await openLogoutConfirmation(page);
  id('confirmLogout').click();
  await until(() => !id('onboarding').hidden);
  id('existingProfile').click();
  id('loginKey').value = Buffer.from(secret).toString('hex').toUpperCase();
  dispatch('loginKey', 'input');
  await until(() => !!page.document.getElementById('skipSetup'));
  id('skipSetup').click();
  await until(() => !id('workspace').hidden);
  assert.equal((await success('status')).conf.pubkey, owner);
  assert.equal((await success('status')).unlocked, true);
  await success('logout', { pubkey: owner });
});
test('manual sync downloads all own signed kinds with automatic import off', async () => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  await success('watch', { pubkey: owner });
  await until(async () => !(await success('status')).running);
  await success('settings', {
    pubkey: owner,
    relays: '',
    handle: '',
    capture: false,
    relaySync: false,
  });
  assert.deepEqual((await success('status')).conf.relays, []);
  const before = (await success('status')).lastRun;
  const empty = await call('sync');
  assert.equal(empty.ok, false);
  assert.ok(!empty.ok && empty.error === 'Select a relay first.');
  assert.deepEqual((await success('status')).lastRun, before);
  const url = 'wss://manual-sync.example/';
  const relay = fixtureRelay(url);
  const kinds = [0, 1, 3, 4, 7, 10002, 22242, 30078, 65535];
  for (const kind of kinds) {
    const event = finalizeEvent(
      {
        kind,
        created_at: 10,
        tags: [],
        content:
          kind === 0 ? '{"name":"manual","display_name":"Manual sync"}' : 'Imported manually',
      },
      secret,
    );
    relay.stored.set(event.id, event);
  }
  const foreign = finalizeEvent({ kind: 7, created_at: 10, tags: [], content: '+' }, generateSecretKey());
  relay.stored.set(foreign.id, foreign);
  await success('settings', {
    pubkey: owner,
    relays: url,
    handle: '',
    capture: false,
    relaySync: false,
  });
  await until(async () => !(await success('status')).running);
  await success('sync');
  await until(
    async () => !(await success('status')).running && (await success('status')).events === kinds.length,
  );
  const status = await success('status');
  assert.equal(status.conf.relaySync, false);
  assert.equal(status.profile?.displayName, 'Manual sync');
  assert.match(status.lastRun!.reports[url], /9 new|9 imported/);
  assert.deepEqual(relay.filters.at(-1)?.authors, [owner]);
  assert.equal(relay.filters.at(-1)?.kinds, undefined);
  assert.equal(relay.sent.length, 0);
  const db = await openArchive(status.conf.archiveName);
  const archived = await db.getAll('events');
  assert.deepEqual(new Set(archived.map(e => e.kind)), new Set(kinds));
  for (const event of archived) assert.deepEqual(event, JSON.parse(JSON.stringify(relay.stored.get(event.id))));
  const { queueBroadcast } = await import('../src/archive.ts');
  assert.equal(await queueBroadcast(db, status.conf), 2);
  db.close();
  await success('logout', { pubkey: owner });
});
test('saved include and exclude selections rescan historical events through either relay protocol', async (t) => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const url = 'wss://kind-selection.example/';
  const relay = fixtureRelay(url);
  const store = (kind: number, time: number) => {
    const event = finalizeEvent({ kind, created_at: time, tags: [], content: 'fixture' }, secret);
    relay.stored.set(event.id, event);
    return event.id;
  };
  const note = store(1, 8), reaction = store(7, 7), contacts = store(3, 6);
  const conf = { pubkey: owner, handle: '', relays: url, capture: false, relaySync: true };
  await success('watch', { pubkey: owner });
  await idle();
  const db = await openArchive((await success('status')).conf.archiveName);
  try {
    // Simulate an installation that completed a post-only scan and isn't due for another day.
    await db.put('cursors', { through: Math.floor(Date.now() / 1000) }, [owner, url]);
    await db.put('meta', Date.now() + 86400000, ['nextPoll', url]);
    await success('settings', { ...conf, relayKinds: { mode: 'include', list: '7, 7' } });
    await idle();
    assert.deepEqual(await db.getAllKeys('events'), [reaction]);
    assert.deepEqual(relay.filters.at(-1)?.kinds, [7]);
    assert.equal(relay.filters.at(-1)?.since, 0);
    assert.deepEqual(relay.reads.at(-1)?.kinds, [7]);

    const page = await archivePage(t), el = pageElements(page);
    await until(() => el('kindMode').value === 'include');
    assert.equal(el('downloadKinds').value, '7');
    // Fields are bound to page state, so set them the way a user does.
    el('downloadKinds').value = '1';
    page.document.getElementById('downloadKinds')!.dispatchEvent(new page.Event('input', { bubbles: true }));
    el('kindMode').value = 'exclude';
    page.document.getElementById('kindMode')!.dispatchEvent(new page.Event('change', { bubbles: true }));
    await until(async () => (await success('status')).conf.relayKinds?.mode === 'exclude');
    await idle();
    assert.deepEqual(new Set(await db.getAllKeys('events')), new Set([reaction, contacts]));
    assert.equal(relay.filters.at(-1)?.kinds, undefined);
    assert.equal(relay.filters.at(-1)?.since, 0);
    assert.equal(await db.count('outbox'), 0);
    const invalid = await call('settings', { ...conf, relayKinds: { mode: 'include', list: '1.5' } });
    assert.equal(invalid.ok, false);
    assert.deepEqual((await success('status')).conf.relayKinds, { mode: 'exclude', kinds: [1] });

    relay.supported = false;
    await success('settings', { ...conf, relayKinds: { mode: 'all', list: '' } });
    await idle();
    assert.deepEqual(new Set(await db.getAllKeys('events')), new Set([reaction, contacts, note]));
    assert.equal(relay.reads.at(-1)?.kinds, undefined);
    const other = store(65535, 4);
    await success('settings', { ...conf, relayKinds: { mode: 'include', list: '65535' } });
    await idle();
    assert.ok(await db.getKey('events', other));
    assert.deepEqual(relay.reads.at(-1)?.kinds, [65535]);
    assert.equal(relay.sent.length, 0);

    // A filtered import has no evidence about queued posts outside its selection.
    relay.supported = true;
    relay.stored.delete(note);
    assert.equal(await success('broadcast'), 1);
    await idle();
    assert.deepEqual(relay.sent.map(e => e.id), [note]);
    assert.equal(await db.count('outbox'), 0);
  } finally {
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('read-only mode pauses capture and preserves existing drafts until the signer unlocks', async (t) => {
  t.mock.method(globalThis, 'fetch', (url: URL) => publicTweet(url, 'Text to archive'));
  const subscriptionStart = subscriptions.length;
  assert.equal((await call('settings', { pubkey }, x)).ok, false);
  assert.equal(
    (
      await call('settings', {
        pubkey,
        handle: 'alice',
        relays: 'wss://fixture.example',
        relaySync: true,
        capture: true,
      })
    ).ok,
    false,
  );
  await success('login', { key: nsecEncode(key) });
  await success('settings', {
    pubkey,
    handle: 'alice',
    relays: 'wss://fixture.example',
    relaySync: true,
    capture: true,
  });
  await until(async () => !(await success('status')).running);
  await success('lock');
  const post = { author: 'alice', id: '987', text: 'Text to archive', time: 1700000000, visibility: 'public' as const };
  assert.equal((await call('capture', { posts: [post] }, { ...x, frameId: 1 })).ok, false);
  assert.equal((await success('captureConfig', {}, x)).capture, false);
  assert.equal((await call('capture', { posts: [post] }, x)).ok, false);
  const drafts = await openArchive('nostr-backup-' + pubkey);
  await stagePosts(drafts, [post, post]);
  drafts.close();
  await until(async () => !(await success('status')).running);
  let status = await success('status');
  assert.equal(status.drafts, 1);
  assert.equal(status.events, 1);
  assert.equal(transmissions.length, 0);
  assert.deepEqual(status.x, { captured: 1, signed: 0, matched: 0, drafts: 1 });
  assert.equal((await call('unlock', { key: nsecEncode(generateSecretKey()) })).ok, false);
  await success('unlock', { key: nsecEncode(key) });
  await until(async () => !(await success('status')).running);
  status = await success('status');
  assert.equal(status.drafts, 0);
  assert.equal(status.events, 2);
  assert.equal(status.pending, 1);
  assert.deepEqual(status.x, { captured: 1, signed: 1, matched: 0, drafts: 0 });
  assert.equal(transmissions.length, 1);
  assert.ok(verifyEvent(structuredClone(transmissions[0])));
  assert.ok(
    subscriptions
      .slice(subscriptionStart)
      .every((f) => f.authors?.length === 1 && f.authors[0] === pubkey),
  );
  accept = true;
  await success('sync');
  await until(async () => !(await success('status')).running);
  assert.equal((await success('status')).pending, 0);
  assert.deepEqual(transmissions[1], transmissions[0]);
  await success('capture', { posts: [post] }, x);
  await until(async () => !(await success('status')).running);
  assert.equal(transmissions.length, 2);
  t.mock.method(api.tabs, 'query', async () => [{ id: 1, url: 'https://x.com/alice' }]);
  const popup = await extensionPage(t, 'popup');
  const id = pageElements(popup);
  // On X the popup says in one line what the backup does there, and gives one number.
  await until(() => !id('siteState').hidden);
  assert.equal(id('siteState').textContent, 'Backing up @alice on X');
  assert.equal(id('siteState').getAttribute('data-level'), 'on');
  assert.equal(id('summary').textContent, '1 post saved from X');
  assert.equal(popup.document.getElementById('xPopupStats'), null);
  assert.equal(id('profileMode').hidden, true);
  // The page in the tab reports a post that stays out of the backup: the popup says so.
  await success('pageStatus', { unsaved: 1 }, x);
  await until(() => id('siteState').getAttribute('data-level') === 'problem');
  assert.equal(id('siteState').textContent, '1 post on this page could not be backed up');
  // Read-only mode is known from the settings, whatever the page last reported.
  await success('lock');
  const lockedPopup = await extensionPage(t, 'popup');
  const locked = pageElements(lockedPopup);
  await until(() => !locked('backupActions').hidden);
  assert.equal(locked('profileMode').hidden, false);
  assert.equal(locked('siteState').textContent, 'X backup is paused. Enter your secret key in Keys & access');
  assert.equal(locked('siteState').getAttribute('data-level'), 'attention');
});
test('signer binds public-key access and approvals to the authorized website', async () => {
  const token = crypto.randomUUID(),
    origin = 'https://client.example';
  const sender = { id: extensionId, url: origin + '/', tab: { id: 8 }, frameId: 0 };
  await api.storage.session.set({ 'grant-8': { token, origin } });
  await success('unlock', { key: nsecEncode(key) });
  const req = (method: string, event?: object, extra: object = {}) =>
    call('nip07', { token, method, event, ...extra }, sender);
  winURL = undefined;
  const loginRequest = req('getPublicKey');
  await until(() => !!winURL);
  const loginScreen = approvalScreen();
  assert.equal((await call('approval', { id: loginScreen.url.split('#')[1], decision: true, scope: 'signing' }, loginScreen)).ok, false);
  await success('approval', { id: loginScreen.url.split('#')[1], decision: true }, loginScreen);
  assert.deepEqual(await loginRequest, { ok: true, value: pubkey });
  assert.equal((await req('signEvent', {}, { token: 'forged' })).ok, false);
  assert.equal(
    (
      await call(
        'nip07',
        { token, method: 'getPublicKey' },
        { ...sender, url: 'https://elsewhere.example/' },
      )
    ).ok,
    false,
  );
  const event = {
    kind: 27235,
    created_at: 12345,
    tags: [
      ['u', origin + '/login'],
      ['method', 'GET'],
    ],
    content: '',
  };
  for (const allow of [false, true]) {
    winURL = undefined;
    const pending = req('signEvent', event);
    await until(() => !!winURL);
    const screen = approvalScreen(),
      id = screen.url.split('#')[1];
    assert.equal((await call('approval', { id, decision: true }, sender)).ok, false);
    const preview = await success('approval', { id }, screen);
    assert.ok(preview);
    assert.deepEqual(preview.event, event);
    await success('approval', { id, decision: allow }, screen);
    const response = await pending;
    assert.equal(response.ok, allow);
    if (allow) {
      assert.ok(response.ok && typeof response.value === 'object');
      assert.equal(response.value.pubkey, pubkey);
      assert.ok(verifyEvent(response.value));
    }
    assert.equal((await call('approval', { id, decision: true }, screen)).ok, false);
  }
  winURL = undefined;
  const post = { ...event, kind: 1 };
  const first = req('signEvent', post);
  await until(() => !!winURL);
  const screen = approvalScreen();
  await success('approval', { id: screen.url.split('#')[1], decision: true, scope: 'kind' }, screen);
  assert.equal((await first).ok, true);
  await api.storage.session.clear();
  assert.equal((await success('status')).unlocked, true);
  assert.equal((await req('getPublicKey')).ok, false);
  await api.storage.session.set({ 'grant-8': { token, origin } });
  winURL = undefined;
  const second = await req('signEvent', { ...post, content: 'Next post' });
  assert.ok(second.ok && typeof second.value === 'object' && verifyEvent(second.value));
  assert.equal(winURL, undefined);
  assert.deepEqual(await success('listPermissions', { account: pubkey }), [{ origin, kind: 1, allow: true, label: 'posts' }]);
  for (const denied of [{ ...sender, origin: 'null' }, { ...sender, frameId: 1 }, { ...sender, origin: 'https://other.example' }])
    assert.equal((await call('nip07', { token, method: 'signEvent', event: post }, denied)).ok, false);
  const sensitive = req('signEvent', event);
  await until(() => !!winURL);
  const sensitiveScreen = approvalScreen();
  await success('approval', { id: sensitiveScreen.url.split('#')[1], decision: false }, sensitiveScreen);
  assert.equal((await sensitive).ok, false);
  // A remembered decline stops the website asking for that kind.
  const relayAuth = { ...event, kind: 22242, tags: [['relay', 'wss://relay.example/'], ['challenge', 'c']] };
  winURL = undefined;
  const declined = req('signEvent', relayAuth);
  await until(() => !!winURL);
  const declineScreen = approvalScreen();
  await success('approval', { id: declineScreen.url.split('#')[1], decision: false, scope: 'kind' }, declineScreen);
  assert.equal((await declined).ok, false);
  winURL = undefined;
  assert.equal((await req('signEvent', { ...relayAuth, created_at: 12346 })).ok, false);
  assert.equal(winURL, undefined);
  assert.deepEqual((await success('listPermissions', { account: pubkey })).map((rule) => [rule.kind, rule.allow, rule.label]),
    [[1, true, 'posts'], [22242, false, 'relay sign-ins']]);
  // Remembering posts has not granted profile changes.
  winURL = undefined;
  const profileChange = req('signEvent', { ...event, kind: 0, tags: [] });
  await until(() => !!winURL);
  const profileScreen = approvalScreen();
  await success('approval', { id: profileScreen.url.split('#')[1], decision: false, scope: 'kind' }, profileScreen);
  assert.equal((await profileChange).ok, false);
  winURL = undefined;
  assert.equal((await req('signEvent', { ...event, kind: 0 })).ok, false);
  assert.equal(winURL, undefined);

  // A whole-site signing decision replaces signing rules only. Test both choices.
  for (const allow of [true, false]) {
    winURL = undefined;
    const login = req('getPublicKey');
    await until(() => !!winURL);
    const loginPrompt = approvalScreen();
    await success('approval', { id: loginPrompt.url.split('#')[1], decision: false, scope: 'kind' }, loginPrompt);
    assert.equal((await login).ok, false);

    winURL = undefined;
    const sign = req('signEvent', event);
    await until(() => !!winURL);
    const signPrompt = approvalScreen();
    await success('approval', { id: signPrompt.url.split('#')[1], decision: allow, scope: 'signing' }, signPrompt);
    assert.equal((await sign).ok, allow);
    winURL = undefined;
    for (const kind of [0, 1, 3, 5, 7, 10002, 65535]) {
      const result = await req('signEvent', { ...event, kind });
      assert.equal(result.ok, allow);
      if (allow) assert.ok(result.ok && typeof result.value === 'object' && verifyEvent(result.value));
    }
    assert.equal((await req('getPublicKey')).ok, false);
    assert.equal(winURL, undefined);
    assert.deepEqual((await success('listPermissions', { account: pubkey })).map(r => [r.label, r.allow]),
      [['all signing requests', allow], ['login', false]]);
    assert.equal(await permissionDecision(pubkey, 'https://other.example', 7), undefined);
    assert.equal(await permissionDecision('other-account', origin, 7), undefined);
    await success('forgetPermissions', { account: pubkey, origin });
    assert.equal(await permissionDecision(pubkey, origin, 7), undefined);
  }
  winURL = undefined;
  const cancelled = req('signEvent', event);
  await until(() => !!winURL);
  const cancelledScreen = approvalScreen();
  await success('forgetPermissions', { account: pubkey, origin });
  assert.equal((await cancelled).ok, false);
  assert.equal((await call('approval', {id: cancelledScreen.url.split('#')[1], decision: true}, cancelledScreen)).ok, false);
  assert.deepEqual(await success('listPermissions', { account: pubkey }), []);
  await success('lock');
  assert.equal((await req('getPublicKey')).ok, false);
});
test('the extension injects window.nostr and the page signs a login event through the approval screen', async (t) => {
  const origin = 'https://client.example';
  const tab = { id: 8, url: origin + '/' };
  const site = new Window({ url: tab.url });
  t.after(() => site.happyDOM.abort());
  // Happy DOM reports its host Window as the sender. A browser exposes the
  // page's WindowProxy to handlers running inside the page.
  const pageWindow = site.eval('window');
  site.addEventListener(
    'message',
    (event) => {
      if ('source' in event && event.source === site)
        Object.defineProperty(event, 'source', { value: pageWindow });
    },
    true,
  );
  await success('login', { key: nsecEncode(key) });
  const sender = { id: extensionId, tab, url: tab.url, frameId: 0 };
  const bridge = {
    chrome: {
      runtime: { sendMessage: ({ type, ...body }: Message) => call(type, body, sender) },
    },
  };
  Object.defineProperty(site, 'chrome', { value: bridge.chrome });
  for (const entry of ['bridge', 'provider']) {
    const bundle = await build({ entryPoints: ['src/' + entry + '.ts'], bundle: true, write: false, format: 'iife' });
    site.eval(bundle.outputFiles[0].text);
  }
  winURL = undefined;
  const loginRequest = site.eval('window.nostr.getPublicKey()');
  await until(() => !!winURL);
  const loginReview = await extensionPage(t, 'approve', undefined, approvalScreen().url);
  await until(() => !pageElements(loginReview)('allow').disabled);
  assert.equal(pageElements(loginReview)('allow').textContent, 'Share user ID');
  const loginScope = loginReview.document.querySelector('select')!;
  assert.deepEqual(Array.from(loginScope.options, option => option.value), ['once', 'kind']);
  pageElements(loginReview)('allow').click();
  assert.equal(await loginRequest, pubkey);
  const login = {
    kind: 27235,
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ['u', origin + '/login'],
      ['method', 'POST'],
    ],
    content: '',
  };
  winURL = undefined;
  const pending: Promise<NostrEvent> = site.eval(
    'window.nostr.signEvent(' + JSON.stringify(login) + ')',
  );
  await until(() => !!winURL);
  const review = await extensionPage(t, 'approve', undefined, approvalScreen().url);
  await until(() => !pageElements(review)('allow').disabled);
  assert.ok(pageElements(review)('origin').textContent.includes(origin));
  assert.deepEqual(JSON.parse(pageElements(review)('event').textContent), login);
  const signingScope = review.document.querySelector('select')!;
  assert.equal(signingScope.value, 'once');
  assert.deepEqual(Array.from(signingScope.options, option => option.value), ['once', 'kind', 'signing']);
  signingScope.value = 'signing';
  signingScope.dispatchEvent(new review.Event('change', { bubbles: true }));
  await until(() => pageElements(review)('allow').textContent === 'Always allow');
  assert.match(pageElements(review)('scopeHint').textContent, /deletions/);
  pageElements(review)('allow').click();
  const signed = await pending;
  assert.equal(signed.pubkey, pubkey);
  assert.deepEqual(
    { ...login, id: signed.id, pubkey, sig: signed.sig },
    JSON.parse(JSON.stringify(signed)),
  );
  assert.ok(verifyEvent(structuredClone(signed)));
  assert.equal(await permissionDecision(pubkey, origin, 5), true);
  await success('forgetPermissions', { account: pubkey, origin });
  await success('lock');
  await assert.rejects(site.eval('window.nostr.getPublicKey()'), /locked/);
});
function installPageStorage(page: Window) {
  for (const [name, value] of Object.entries({
    indexedDB,
    IDBKeyRange,
    IDBRequest,
    IDBDatabase,
    IDBObjectStore,
    IDBIndex,
    IDBCursor,
    IDBTransaction,
    structuredClone,
    crypto,
    Uint8Array,
    ArrayBuffer,
    Blob,
  }))
    Object.defineProperty(page, name, { value, configurable: true });
  Object.defineProperty(page.navigator, 'locks', {
    value: { request: (_: string, __: object, fn: (lock: object) => unknown) => fn({}) },
  });
}
async function openLogoutConfirmation(page: Window) {
  page.location.hash = '#account';
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => page.document.querySelector('nav a[href="#account"]')?.getAttribute('aria-current') === 'page');
  pageElements(page)('logout').click();
  await until(() => !!page.document.getElementById('confirmLogout'));
}

function pageElements(page: Window): typeof import('../src/ui.ts').el {
  return ((id: string) => {
    const element = page.document.getElementById(id);
    assert.ok(element, 'Missing fixture element ' + id);
    return element;
  }) as unknown as typeof import('../src/ui.ts').el;
}
function archivePage(t: TestContext, setup?: (page: Window) => void) {
  return extensionPage(t, 'archive', setup);
}
async function extensionPage(
  t: TestContext,
  name: 'archive' | 'approve' | 'popup',
  setup?: (page: Window) => void,
  url = ext + name + '.html',
) {
  const page = new Window({
    url,
    settings: { disableCSSFileLoading: true, disableJavaScriptFileLoading: true },
  });
  t.after(() => page.happyDOM.abort());
  Object.defineProperty(page, 'chrome', {
    value: {
      storage: { local: api.storage.local, session: api.storage.session },
      tabs: { query: () => api.tabs.query() },
      runtime: {
        openOptionsPage: () => api.runtime.openOptionsPage(),
        sendMessage: ({ type, ...body }: Message) => call(type, body, { ...ui, url }),
      },
    },
  });
  page.document.documentElement.innerHTML = await readFile('ui/' + name + '.html', 'utf8');
  const bundle = await build({
    entryPoints: ['src/' + (name === 'archive' ? 'archive-ui' : name) + '.ts'],
    bundle: true,
    write: false,
    format: 'iife',
    plugins: [svelte],
  });
  setup?.(page);
  page.eval(bundle.outputFiles[0].text);
  return page;
}
test('archive button reconciles each relay, falls back and retries with capture and signing off', async (t) => {
  const db = await openArchive('nostr-backup-' + pubkey);
  const capable = 'wss://neg.example/',
    plain = 'wss://plain.example/';
  const relays = [capable, plain];
  for (const url of relays)
    broadcastRelays.set(url, {
      supported: url === capable,
      sent: [],
      stored: new Map(),
      filters: [],
      reads: [],
    });
  const neg = broadcastRelays.get(capable)!,
    fallback = broadcastRelays.get(plain)!;
  const configure = (urls: string[]) =>
    success('settings', {
      pubkey,
      handle: '',
      relays: urls.join('\n'),
      capture: false,
      relaySync: false,
    });
  const idle = () => until(async () => !(await success('status')).running);
  await success('lock');
  await configure([]);
  await idle();
  assert.equal((await call('broadcast')).ok, false);
  await configure(relays);
  await idle();
  const copy = finalizeEvent(
    { kind: 1, created_at: 99, tags: [], content: 'Signed X copy' },
    key,
  );
  await append(db, [{ event: native }, { event: copy, source: 'x:99', targets: [] }], pubkey);
  await stagePosts(db, [{ author: 'alice', id: '876', time: 90, text: 'Unsigned', visibility: 'public' }]);
  const archive = await db.getAll('events');
  const ids = archive.map((event) => event.id).sort();
  neg.stored.set(native.id, native);
  fallback.reject = copy.id;
  assert.equal((await call('broadcast', {}, x)).ok, false);
  const page = await archivePage(t);
  const button = pageElements(page)('broadcast');
  await until(() => !button.disabled);
  button.click();
  await until(() => pageElements(page)('notice').textContent === 'Posts queued for sending.');
  await idle();
  assert.deepEqual(
    neg.sent.map((e) => e.id).sort(),
    ids.filter((id) => id !== native.id),
  );
  assert.deepEqual(fallback.sent.map((e) => e.id).sort(), ids);
  for (const dest of [neg, fallback]) {
    assert.deepEqual(dest.filters[0].authors, [pubkey]);
    for (const e of dest.sent)
      assert.deepEqual(
        e,
        archive.find((event) => event.id === e.id),
      );
  }
  let status = await success('status');
  assert.match(status.lastRun!.reports[capable], /1 already on relay/);
  assert.match(status.lastRun!.reports[plain], /pending retry/);
  assert.equal(status.pending, 1);
  await configure([capable]);
  await idle();
  // A removed relay keeps its queued copy for later, but it no longer counts as waiting.
  assert.equal((await success('status')).pending, 0);
  assert.equal(await db.count('outbox'), 1);
  fallback.reject = undefined;
  await configure(relays);
  await idle();
  assert.deepEqual(
    fallback.sent.at(-1),
    archive.find((e) => e.id === copy.id),
  );
  assert.equal((await success('status')).pending, 0);
  await configure([capable]);
  await idle();
  await success('broadcast');
  await idle();
  assert.equal(neg.sent.length, ids.length - 1);
  status = await success('status');
  assert.equal(status.drafts, 1);
  assert.equal(status.pending, 0);
  assert.equal(status.unlocked, false);
  db.close();
});
const idle = () => until(async () => !(await success('status')).running);
test('the outbox stores a deadline even when the relay omits a delay', (t) => {
  t.mock.method(Date, 'now', () => 10000);
  assert.equal(retryDeadline({ retryAfterMs: 2500 }), 12500);
  assert.equal(retryDeadline({}), 70000);
});
async function configureRelays(urls: string[], relaySync: boolean) {
  await success('settings', {
    pubkey,
    handle: '',
    relays: urls.join('\n'),
    capture: false,
    relaySync,
  });
  await idle();
}
function fixtureRelay(url: string, supported = true) {
  const dest: FixtureRelay = {
    supported,
    sent: [],
    stored: new Map(),
    filters: [],
    reads: [],
  };
  broadcastRelays.set(url, dest);
  return dest;
}
const note = (content: string, created_at: number) =>
  finalizeEvent({ kind: 1, created_at, tags: [], content }, key);

test('two-way sync restores an empty archive, reconciles both sides and retries incomplete downloads', async (t) => {
  const db = await openArchive('nostr-backup-' + pubkey);
  t.after(() => db.close());
  for (const name of db.objectStoreNames) await db.clear(name);
  const url = 'wss://both.example/';
  const relay = fixtureRelay(url);
  await configureRelays([url], true);
  const older = note('old relay-only post', 1);
  relay.stored.set(older.id, older);
  // The previous scheduled import completed. A manual run still covers time zero.
  const page = await archivePage(t);
  const button = pageElements(page)('broadcast');
  await until(() => !button.disabled);
  assert.equal(button.textContent, 'Resend posts');
  button.click();
  await until(() =>
    pageElements(page)('notice').textContent.includes('Checking saved relays'),
  );
  await idle();
  assert.equal((await db.get('events', older.id))?.sig, older.sig);
  assert.equal(relay.filters.at(-1)?.since, 0);
  assert.deepEqual(relay.reads.at(-1)?.ids, [older.id]);
  assert.equal(relay.sent.length, 0);

  const local = note('local only', 200);
  await append(db, [{ event: local }], pubkey);
  const remote = Array.from({ length: 101 }, (_, i) => note('remote ' + i, i + 400));
  for (const event of remote) relay.stored.set(event.id, event);
  relay.withheld = remote.at(-1)!.id;
  const start = relay.filters.length;
  await success('broadcast');
  await idle();
  assert.equal(relay.filters.length, start + 1);
  assert.ok(relay.reads.every((f) => f.ids && f.ids.length <= 100));
  assert.deepEqual(
    relay.sent.map((e) => e.id),
    [local.id],
  );
  assert.equal(await db.get('cursors', [pubkey, url]), undefined);
  assert.match((await success('status')).lastRun!.reports[url], /Incomplete/);
  relay.withheld = undefined;
  await success('sync');
  await idle();
  assert.equal(await db.count('events'), 2 + remote.length);
  assert.equal(await db.count('outbox'), 0);
  assert.ok(((await db.get('cursors', [pubkey, url]))?.through ?? 0) > 0);

  const extra = note('never published by scheduled imports', 500);
  await append(db, [{ event: extra }], pubkey);
  const now = Math.floor(Date.now() / 1000);
  const fresh = note('new relay event', now);
  relay.stored.set(fresh.id, fresh);
  await success('sync');
  await idle();
  assert.equal((await db.get('events', fresh.id))?.sig, fresh.sig);
  assert.deepEqual(
    relay.sent.map((e) => e.id),
    [local.id],
  );
  assert.equal(relay.reads.at(-1)?.ids?.length, 1);
});

test('relay cooldown survives immediate syncs and lets other relays finish', async (t) => {
  const db = await openArchive('nostr-backup-' + pubkey);
  t.after(() => db.close());
  for (const name of db.objectStoreNames) await db.clear(name);
  const slow = 'wss://slow.example/',
    fast = 'wss://fast.example/';
  const limited = fixtureRelay(slow),
    healthy = fixtureRelay(fast, false);
  await configureRelays([slow, fast], false);
  const events = [note('pending one', 2), note('pending two', 3)];
  await append(
    db,
    events.map((event) => ({ event })),
    pubkey,
  );
  const count = await db.count('events');
  const epoch = Date.now();
  let now = epoch;
  t.mock.method(Date, 'now', () => now);
  const alarms = new Map();
  t.mock.method(api.alarms, 'create', async (name: string, options: { when: number }) =>
    alarms.set(name, options.when),
  );
  limited.publishLimit = ['rate-limited: slow down', '125000'];
  await success('broadcast');
  await idle();
  const cooldown = ['retryAfter', slow];
  assert.equal(await db.get('meta', cooldown), epoch + 125000);
  assert.equal(alarms.get('relay-retry:' + slow), epoch + 125000);
  assert.equal(limited.sent.length, 1);
  assert.equal(healthy.sent.length, count);
  assert.equal(await db.count('outbox'), count);
  const limitedStatus = (await success('status')).relays.find((relay) => relay.url === slow)!;
  assert.equal(limitedStatus.phase, 'retry');
  assert.equal(limitedStatus.retryAt, epoch + 125000);
  assert.equal(limitedStatus.pending, count);
  limited.publishLimit = undefined;
  now += 124999;
  await success('sync');
  await idle();
  assert.equal(limited.sent.length, 1);
  now++;
  api.alarms.onAlarm.list[0]({ name: 'relay-retry:' + slow });
  await idle();
  assert.equal(limited.sent.length, count + 1);
  assert.equal(await db.count('outbox'), 0);

  const sent = limited.sent.length;
  limited.negLimit = ['rate-limited: quota exceeded; retry in 4s'];
  await configureRelays([slow], false);
  await success('broadcast');
  await idle();
  assert.equal(limited.sent.length, sent);
  assert.equal(await db.count('outbox'), count);
  assert.equal(await db.get('meta', cooldown), now + 4000);
  now += 4000;
  limited.negLimit = undefined;
  limited.supported = false;
  limited.readLimit = ['rate-limited: reduce requests', '90000'];
  await configureRelays([slow], true);
  assert.equal(await db.get('meta', cooldown), now + 90000);
  assert.equal(limited.sent.length, sent);
  now += 90000;
  limited.readLimit = undefined;
  const remote = note('old post on fallback relay', 12);
  limited.stored.set(remote.id, remote);
  await success('broadcast');
  await idle();
  assert.equal((await db.get('events', remote.id))?.sig, remote.sig);
  assert.equal(await db.count('outbox'), 0);
});
test('tabbed archive backs up a media attachment and scopes uploads to its saved server', async (t) => {
  const server = 'https://media.example',
    mediaURL = 'https://files.example/test.png';
  const db = await openArchive('nostr-backup-' + pubkey);
  t.after(() => db.close());
  await success('settings', {
    pubkey,
    relays: '',
    handle: '',
    blossomServer: server,
    mediaMiB: 8,
  });
  const note = finalizeEvent({ kind: 1, content: mediaURL, tags: [], created_at: 17 }, key);
  await append(db, [{ event: note }], pubkey);
  await success('lock');
  const page = await archivePage(t, (page) => {
    installPageStorage(page);
    const fetcher: typeof fetch = async (url, init) => {
      if (url === mediaURL)
        return new Response('media bytes', { headers: { 'Content-Type': 'image/png' } });
      if (init?.method === 'HEAD') return new Response(null, {status:404});
      assert.equal(url, server + '/upload');
      const auth = JSON.parse(
        Buffer.from(
          new Headers(init?.headers).get('Authorization')!.slice(6),
          'base64',
        ).toString(),
      );
      assert.ok(verifyEvent(auth));
      assert.equal(auth.kind, 24242);
      assert.ok(auth.tags.some((t) => t[0] === 'server' && t[1] === 'media.example'));
      const hash = new Headers(init?.headers).get('X-SHA-256');
      assert.ok(auth.tags.some((t) => t[0] === 'x' && t[1] === hash));
      assert.ok(init?.body instanceof Uint8Array);
      return Response.json({
        sha256: hash,
        size: init.body.length,
        url: server + '/' + hash,
      });
    };
    Object.defineProperty(page, 'fetch', { value: fetcher });
    t.mock.method(globalThis, 'fetch', fetcher);
  });
  const id = pageElements(page);
  const tab = (hash: string) => {
    page.location.hash = hash;
    page.dispatchEvent(new page.Event('hashchange'));
  };
  tab('#account');
  await until(() => id('blossomServer').value === server);
  assert.equal(
    page.document.querySelector('[data-panel="archive"]')?.hasAttribute('hidden'),
    true,
  );
  assert.equal(
    page.document.querySelector('a[aria-current="page"]')?.getAttribute('href'),
    '#account',
  );
  assert.equal(page.document.querySelector('nav a[href="#profile"]')?.textContent, 'Profile');
  assert.equal(page.document.querySelector('nav a[href="#account"]')?.textContent, 'Keys & access');
  assert.equal(page.document.querySelector('[data-panel="account"] h2')?.textContent, 'Keys & access');
  // Old X and Instagram & Facebook links land on the merged Sources page.
  for (const old of ['#x', '#profiles']) {
    tab(old);
    assert.equal(page.location.hash, '#sources');
    await until(() => page.document.querySelector('a[aria-current="page"]')?.getAttribute('href') === '#sources');
    assert.equal(page.document.querySelector('[data-panel="sources"]')?.hasAttribute('hidden'), false);
    tab('#account');
    await until(() => page.document.querySelector('a[aria-current="page"]')?.getAttribute('href') === '#account');
  }
  tab('#media');
  await until(() => !id('backupMedia').disabled);
  id('backupMedia').click();
  await until(() => id('notice').textContent);
  assert.match(id('notice').textContent, /1 saved/);
  await until(() => id('savedFiles').textContent === '1');
  assert.equal(id('uploadMedia').disabled, true);
  const [file] = await db.getAll('mediaFiles');
  assert.equal((await call('authorizeMediaUpload', { server, hash: file.sha256 })).ok, false);
  await success('unlock', { key: nsecEncode(key) });
  assert.equal(
    (
      await call('authorizeMediaUpload', {
        server: 'https://elsewhere.example',
        hash: file.sha256,
      })
    ).ok,
    false,
  );
  assert.equal(
    (await call('authorizeMediaUpload', { server, hash: file.sha256 }, x)).ok,
    false,
  );
  assert.equal((await call('status', {}, { ...ui, url: ui.url + '#settings' })).ok, true);
  tab('#media');
  await until(() => !id('uploadMedia').disabled);
  id('uploadMedia').click();
  await until(() => id('notice').textContent === 'Media sync queued.');
  await until(async () => (await db.get('mediaFiles', file.sha256))?.copies.includes(server));
  assert.deepEqual((await db.get('mediaFiles', file.sha256))?.copies, [server]);
});
test('Welcome creates a recoverable profile, logs out, isolates accounts and deletes only the confirmed archive', async (t) => {
  const previous = await success('status');
  if (previous.conf.pubkey) await success('logout', { pubkey: previous.conf.pubkey });
  const page = await archivePage(t, installPageStorage);
  const id = pageElements(page);
  await until(() => !id('onboarding').hidden);
  const destinations = DEFAULT_RELAYS.map(url => fixtureRelay(url));
  const receipt = Promise.withResolvers<void>();
  destinations[0].holdPublish = receipt.promise;
  t.after(() => receipt.resolve());
  // Only the signed-in accounts on already-open tabs fill the first screen.
  let socialTabsOpen = false;
  t.mock.method(api.tabs, 'query', async (query?: { url?: string[] }) => {
    if (!socialTabsOpen) return [];
    const url = query?.url?.[0] ?? '';
    if (url.includes('instagram')) return [{ id: 8, url: 'https://www.instagram.com/' }];
    if (url.includes('facebook')) return [{ id: 9, url: 'https://www.facebook.com/' }];
    return [{ id: 7, url: 'https://x.com/home' }];
  });
  t.mock.method(api.scripting, 'executeScript', async ({ target }: Injection) => [{
    result: target.tabId === 7 ? '/Example' : target.tabId === 9 ? '123456789' : {
      username: 'Example.IG', name: 'Example on Instagram', about: 'Pots and plants', website: '',
      picture: 'https://scontent.cdninstagram.com/v/face.jpg?oe=1',
    },
  }]);
  const profilePage = publicProfileHTML('Example', {
    name: 'Example Person', bio: 'Potter. Shop: https://t.co/shop', links: [['https://t.co/shop', 'https://shop.example/']],
    avatar: 'https://pbs.twimg.com/profile_images/1/face_normal.jpg', banner: 'https://pbs.twimg.com/profile_banners/9/100',
    website: 'https://example.com/',
  });
  const profileFetch = t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    assert.equal(String(input), 'https://x.com/example');
    assert.equal(init?.credentials, 'omit');
    const response = new Response(profilePage, { headers: { 'content-type': 'text/html' } });
    Object.defineProperty(response, 'url', { value: 'https://x.com/Example' });
    return response;
  });
  const setupX = () => page.document.getElementById('setup-x') as unknown as HTMLInputElement;
  id('startSignUp').click();
  await until(() => !id('accountsForm').hidden);
  assert.equal(setupX().value, '');
  // Coming back to the page after opening X looks again.
  socialTabsOpen = true;
  page.dispatchEvent(new page.Event('focus'));
  await until(() => setupX().value === 'example');
  assert.match(page.document.querySelector('#accountsForm .detected')?.textContent ?? '', /Signed in on X/);
  assert.equal((page.document.getElementById('setup-instagram') as unknown as HTMLInputElement).value, 'example.ig');
  assert.equal((page.document.getElementById('setup-facebook') as unknown as HTMLInputElement).value, '123456789');
  // Progress dots count the slides that apply: accounts, profile, Nostr, "You're set" and the key.
  const stepLabel = (selector: string) => page.document.querySelector(selector + ' .wizardSteps')?.getAttribute('aria-label');
  assert.equal(stepLabel('#accountsForm'), 'Step 1 of 5');
  // A cleared field stays empty when the page looks again; the username can be typed back.
  id('clear-x').click();
  await until(() => setupX().value === '' && !page.document.getElementById('clear-x'));
  page.dispatchEvent(new page.Event('focus'));
  await delay(50);
  assert.equal(setupX().value, '');
  setupX().value = 'Example';
  setupX().dispatchEvent(new page.Event('input', { bubbles: true }) as unknown as Event);
  id('accountsForm').requestSubmit();
  // The next screen previews the public profile from X, each detail removable.
  await until(() => !id('nameForm').hidden);
  assert.equal(id('profileName').value, 'Example Person');
  assert.equal(id('profileAbout').value, 'Potter. Shop: https://shop.example/');
  assert.equal(page.document.querySelector('.previewAvatar img')?.getAttribute('src'), 'https://pbs.twimg.com/profile_images/1/face_400x400.jpg');
  assert.equal(page.document.querySelector('.previewBanner img')?.getAttribute('src'), 'https://pbs.twimg.com/profile_banners/9/100/1500x500');
  id('removeWebsite').click();
  await until(() => !page.document.getElementById('removeWebsite'));
  id('profileName').value = 'Example';
  id('profileName').dispatchEvent(new page.Event('input', { bubbles: true }) as unknown as Event);
  profileFetch.mock.restore();
  // The pictures are copied to the Blossom server in the background, so they outlast the X account.
  const jpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 7]);
  const uploads = Promise.withResolvers<void>();
  const copies = t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === DEFAULT_BLOSSOM_SERVER + '/upload') {
      await uploads.promise;
      const hash = (init?.headers as Record<string, string>)['X-SHA-256'];
      return Response.json({ url: DEFAULT_BLOSSOM_SERVER + '/' + hash + '.jpg', sha256: hash, size: jpeg.byteLength });
    }
    assert.match(url, /^https:\/\/pbs\.twimg\.com\//);
    return new Response(jpeg, { headers: { 'content-type': 'image/jpeg' } });
  });
  id('nameForm').requestSubmit();
  // Signing up saves the usernames through the Sources settings, without waiting for the copies.
  // With an X account, the next step links it to Nostr and finds the people you follow.
  await until(() => !!page.document.getElementById('nostrNext'));
  assert.equal(stepLabel('.setup'), 'Step 3 of 5');
  assert.match(page.document.getElementById('linkX')?.getAttribute('href') ?? '', /^https:\/\/x\.com\/intent\/post\?text=Verifying/);
  const setup = page.document.querySelector('.setup')!;
  assert.equal(!!setup.querySelector('#findXFollows'), false);
  const followingURL = 'https://x.com/example/following';
  assert.equal(setup.querySelector('a[href$="/following"]')?.getAttribute('href'), followingURL);
  await success('xFollows', { handles: ['bob'] }, { ...x, url: followingURL, tab: { id: 1, url: followingURL } });
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => !!setup.querySelector('#findXFollows'));
  assert.match(setup.textContent ?? '', /1 account/);
  assert.equal(setup.querySelector('#findXFollows')?.hasAttribute('disabled'), false);
  assert.equal((await success('status')).xLink, undefined);
  id('nostrNext').click();
  await until(() => !!page.document.getElementById('finishSetup'));
  const latestProfile = async () => {
    const d = await openArchive((await success('status')).conf.archiveName);
    try {
      return (await d.getAll('events')).filter((e) => e.kind === 0).sort((a, b) => b.created_at - a.created_at)[0];
    } finally {
      d.close();
    }
  };
  assert.equal(JSON.parse((await latestProfile()).content).picture, 'https://pbs.twimg.com/profile_images/1/face_400x400.jpg');
  // Nor for relays: one is still holding its receipt.
  assert.ok((await success('status')).pending >= 1);
  uploads.resolve();
  const copy = DEFAULT_BLOSSOM_SERVER + '/' + createHash('sha256').update(jpeg).digest('hex') + '.jpg';
  await until(async () => JSON.parse((await latestProfile()).content).picture === copy);
  copies.mock.restore();
  assert.equal((await success('status')).conf.social?.instagram?.handle, 'example.ig');
  assert.equal(id('workspace').hidden, true);
  assert.equal((await success('status')).conf.handle, 'example');
  assert.equal(page.document.querySelector('.setupLinks a')?.getAttribute('href'), 'https://x.com/example');
  let copiedText = '';
  Object.defineProperty(page.navigator, 'clipboard', { value: { writeText: async (text: string) => { copiedText = text; } } });
  // A new account ends setup by offering to save its secret key, which can be skipped.
  id('finishSetup').click();
  await until(() => !!page.document.getElementById('saveKeyFile'));
  assert.equal(stepLabel('.setup'), 'Step 5 of 5');
  assert.match(page.document.querySelector('.setup .hint')?.textContent ?? '', /read\s+your Nostr private messages/);
  assert.equal(id('keyDone').textContent, 'Skip for now');
  id('copyKey').click();
  await until(() => !!copiedText && id('keyDone').textContent === 'Go to your backup');
  const nsec = copiedText;
  assert.equal(page.document.body.textContent.includes(nsec), false);
  id('keyDone').click();
  await until(() => !id('workspace').hidden);
  assert.equal(id('handle').value, 'example');
  assert.equal(page.document.getElementById('recoveryKey'), null);
  const owner = (await success('status')).conf.pubkey;
  copiedText = '';
  id('copyPrivateKey').click();
  await until(() => copiedText === nsec);
  const decoded = decode(nsec);
  assert.equal(decoded.type, 'nsec');
  assert.equal(getPublicKey(decoded.data), owner);
  assert.equal(page.document.body.textContent.includes(nsec), false);
  assert.equal((await call('exportPrivateKey', { account: owner }, x)).ok, false);
  assert.equal((await call('exportPrivateKey', { account: owner }, { ...ui, url: ext + 'popup.html' })).ok, false);
  id('copyProfileId').click();
  await until(() => copiedText === npubEncode(owner));
  assert.equal(page.document.querySelector('[data-panel="account"]')!.textContent.includes(copiedText), false);
  assert.equal(id('indexAddress').value, 'htree://' + npubEncode(owner) + '/my-events');
  const first = await success('status');
  await api.storage.local.set({settings: {...first.conf, pubkey: '0'.repeat(64), capture: true, handle: 'other'}});
  assert.deepEqual((await success('status')).conf, first.conf);
  await api.storage.local.remove('settings');
  assert.ok(first.conf.archiveName);
  assert.equal(first.unlocked, true);
  const profile = await latestProfile();
  assert.deepEqual(JSON.parse(profile.content), {
    name: 'Example',
    lud16: npubEncode(owner) + '@npub.cash',
    about: 'Potter. Shop: https://shop.example/',
    picture: copy,
    banner: copy,
  });
  assert.ok(verifyEvent(profile));
  receipt.resolve();
  await until(() => destinations.every(relay => relay.sent.some(event => event.id === profile.id)));
  await until(async () => (await success('status')).pending === 0);
  const relayUrl = 'wss://index.example/';
  const relay = fixtureRelay(relayUrl);
  const server = 'https://blocks.example';
  let pausedUpload: PromiseWithResolvers<void> | undefined;
  let suspendUploads = false;
  t.mock.method(
    globalThis,
    'fetch',
    async (url: RequestInfo | URL, init: RequestInit = {}) => {
      assert.equal(new URL(String(url)).origin, server);
      if (suspendUploads && init.method === 'PUT') {
        pausedUpload ??= Promise.withResolvers<void>();
        await pausedUpload.promise;
      }
      if (init.method === 'HEAD') return new Response(null, { status: 404 });
      assert.equal(init.method, 'PUT');
      const hash = new Headers(init?.headers).get('X-SHA-256');
      assert.ok(init.body instanceof Blob);
      return Response.json({
        sha256: hash,
        size: init.body.size,
        url: server + '/' + hash,
      });
    },
  );
  for (const [field, value] of [['relays', relayUrl], ['blossomServer', server]]) {
    id(field as 'relays').value = value;
    page.document.getElementById(field)!.dispatchEvent(new page.Event('input', { bubbles: true }));
  }
  id('backupSettings').requestSubmit();
  await until(() => id('notice').textContent === 'Saved.');
  page.location.hash = '#archive';
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => !id('publishIndex').disabled);
  assert.equal(relay.sent.length, 0);
  id('publishIndex').click();
  await until(async () => (await success('status')).pointer?.acknowledged.includes(relayUrl));
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => id('pointerStatus').textContent.includes('Received by ' + relayUrl));
  assert.equal(relay.sent.length, 1);
  assert.equal(relay.sent[0].kind, 30064);
  assert.ok(verifyEvent(relay.sent[0]));
  // The first profile and its update with the picture copies.
  assert.equal((await success('status')).events, 2);
  page.location.hash = '#storage';
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => id('kindList').textContent.includes('Profile'));
  assert.match(id('storageTotals').textContent, /Pieces in the backup index/);
  const details = id('kindList').querySelector('details');
  assert.ok(details);
  details.open = true;
  details.dispatchEvent(new page.Event('toggle') as unknown as Event);
  await until(() => details.textContent.includes(profile.id));
  await api.storage.session.set({ 'grant-99': { token: 'old' } });
  suspendUploads = true;
  const interrupted = call('publishIndex', { account: owner });
  await until(() => !!pausedUpload);
  await openLogoutConfirmation(page);
  id('confirmLogout').click();
  await until(() => id('notice').textContent.includes('Logged out'));
  assert.equal((await interrupted).ok, false);
  suspendUploads = false;
  assert.ok(pausedUpload);
  pausedUpload.resolve();
  assert.equal((await success('status')).events, 0);
  assert.equal((await api.storage.session.get('grant-99'))['grant-99'], undefined);
  assert.equal((await api.storage.session.get('signingKey'))?.signingKey, undefined);
  const other = generateSecretKey(),
    otherPub = getPublicKey(other);
  assert.equal((await call('login', { key: 'bad-key' }, x)).ok, false);
  await success('login', { key: Buffer.from(other).toString('hex') });
  assert.equal((await success('status')).events, 0);
  assert.equal((await call('deleteAccount', { pubkey: owner, confirmed: true })).ok, false);
  assert.equal((await call('deleteAccount', { pubkey: otherPub })).ok, false);
  await success('logout', { pubkey: otherPub });
  await success('watch', { pubkey: npubEncode(owner) });
  assert.equal((await success('status')).events, 2);
  assert.equal((await success('status')).unlocked, false);
  assert.equal((await call('exportPrivateKey', { account: owner })).ok, false);
  await success('login', { key: nsec });
  const local = await openArchive(first.conf.archiveName);
  const { openMediaBlocks } = await import('../src/media-files.ts');
  const stored = openMediaBlocks(local);
  await stored.put(new Uint8Array(32), new Uint8Array([12]));
  stored.close();
  local.close();
  page.location.hash = '#settings';
  page.dispatchEvent(new page.Event('hashchange'));
  await until(() => id('unlock').hidden && !id('copyProfileId').hidden);
  copiedText = '';
  id('copyProfileId').click();
  await until(() => copiedText === npubEncode(owner));
  id('deleteStart').click();
  await until(() => !id('deleteConfirm').hidden);
  id('cancelDelete').click();
  await until(() => id('deleteConfirm').hidden);
  id('deleteStart').click();
  await until(() => !id('deleteConfirm').hidden);
  id('deleteAccount').click();
  await until(() => id('notice').textContent.includes('Local archive deleted.'));
  assert.equal((await success('status')).conf.pubkey, '');
  const names = (await indexedDB.databases()).map((db) => db.name);
  assert.ok(!names.includes(first.conf.archiveName));
  assert.ok(!names.includes(first.conf.archiveName + '-media'));
  assert.ok(names.includes('nostr-backup-' + otherPub));
  await success('login', { key: nsec });
  assert.equal((await success('status')).events, 0);
  await success('logout', { pubkey: owner });
});

test('X captures sign locally even while the previous delivery has no reply', async (t) => {
  t.mock.method(globalThis, 'fetch', (url: URL) =>
    publicTweet(url, 'Post ' + String(url).split('/').pop()));
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const url = 'wss://offline-capture.example/';
  const relay = fixtureRelay(url);
  const held = Promise.withResolvers<void>();
  relay.holdPublish = held.promise;
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url });
    let third = '';
    const post = (id: string) => ({ author: 'alice', id, time: 1700000000, text: 'Post ' + id });
    // X navigates without reloading. Chrome keeps the original document URL but updates the tab URL.
    const fromFollowing = { ...x, url: 'https://x.com/alice/following' };
    await success('capture', { posts: [post('1')] }, fromFollowing);
    assert.equal((await call('capture', { posts: [post('1')] }, { ...x, tab: { id: 1, url: 'https://x.com/bob' } })).ok, false);
    assert.equal((await call('capture', { posts: [post('1')] }, { ...x, url: 'https://example.org/' })).ok, false);
    await until(() => relay.sent.length === 1);
    await success('capture', { posts: [post('2'), post('3'), post('2')] }, x);
    const status = await success('status');
    assert.deepEqual(status.x, { captured: 3, signed: 3, matched: 0, drafts: 0 });
    assert.equal(status.pending, 3);
    const db = await openArchive(status.conf.archiveName);
    try {
      const root = await readRoot(db);
      assert.ok(root);
      const index = new NostrEventStore(readOnlyStore(async hash => (await db.get('blocks', hex(hash))) ?? null));
      for (const id of ['1', '2', '3']) {
        const eventId = await db.get('sources', 'x:' + id);
        assert.ok(eventId);
        const event = await index.getById(root, eventId);
        assert.ok(event && verifyEvent(event));
        assert.equal(event.content, 'Post ' + id);
        third = eventId;
      }
    } finally { db.close(); }
    // Any X page can ask which of its posts are in the backup, to mark them. The answer comes from this device.
    const home = { ...x, url: 'https://x.com/home', tab: { id: 1, url: 'https://x.com/home' } };
    const requests = (globalThis.fetch as unknown as { mock: { callCount(): number } }).mock.callCount();
    assert.equal((await success('captureConfig', {}, home)).marks, true);
    // The tab's toolbar icon gets a green dot while the backup is on, and its tooltip says what it does.
    await until(() => badges.get(1)?.title === 'Social Backup: Backing up @alice on X');
    assert.deepEqual(badges.get(1)?.path, { 16: 'icon-on-16.png', 32: 'icon-on-32.png' });
    // The page reports posts that stay out of the backup: the dot becomes a red mark, and the popup can read why.
    await success('pageStatus', { unsaved: 2 }, home);
    await until(() => badges.get(1)?.title === 'Social Backup: 2 posts on this page could not be backed up');
    assert.equal(badges.get(1)?.path?.[16], 'icon-problem-16.png');
    assert.deepEqual((await api.storage.session.get('site-1'))['site-1'], { site: 'x', unsaved: 2 });
    assert.equal((await success('captureConfig', { failed: true }, home)).capture, true);
    await until(() => badges.get(1)?.title === 'Social Backup: Your posts on this page could not be saved');
    await success('pageStatus', {}, home);
    await until(() => badges.get(1)?.title === 'Social Backup: Backing up @alice on X');
    assert.equal(badges.get(1)?.path?.[16], 'icon-on-16.png');
    assert.deepEqual(await success('savedPosts', { ids: ['1', '9', '3'] }, home), [{ id: '1', onNostr: true }, { id: '3', onNostr: true }]);
    assert.equal(requests, (globalThis.fetch as unknown as { mock: { callCount(): number } }).mock.callCount());
    // A click on a mark has the extension open the post's signed copy on Iris. The page gets no note ID.
    tabURLs.length = 0;
    assert.equal(await success('openSaved', { id: '3' }, home), true);
    assert.equal(await success('openSaved', { id: '9' }, home), false);
    assert.deepEqual(tabURLs, ['https://iris.to/' + noteEncode(third)]);
    assert.equal((await call('openSaved', { id: 'x:1' }, home)).ok, false);
    assert.equal((await call('savedPosts', { ids: ['x:1'] }, home)).ok, false);
    assert.equal((await call('savedPosts', { ids: ['1'] }, { ...x, url: 'https://elsewhere.example/', tab: { id: 2, url: 'https://elsewhere.example/' } })).ok, false);
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url, markSaved: false });
    assert.equal((await success('captureConfig', {}, home)).marks, false);
    // Without marks the page still asks which posts are saved, to see that the backup works.
    assert.deepEqual(await success('savedPosts', { ids: ['1'] }, home), [{ id: '1', onNostr: true }]);
    // A site that is not set up gets no dot.
    await success('captureConfig', {}, { ...x, url: 'https://www.instagram.com/alice/', tab: { id: 31, url: 'https://www.instagram.com/alice/' } });
    await until(() => badges.get(31)?.title === 'Social Backup: Add your Instagram username in Sources');
    assert.equal(badges.get(31)?.path?.[16], 'icon-16.png');
    // In read-only mode the backup is paused: a yellow mark.
    await success('lock');
    assert.equal((await success('captureConfig', {}, home)).capture, false);
    await until(() => badges.get(1)?.title === 'Social Backup: X backup is paused. Enter your secret key in Keys & access');
    assert.equal(badges.get(1)?.path?.[16], 'icon-attention-16.png');
    // A closed tab's state is forgotten.
    for (const closed of api.tabs.onRemoved.list) closed(1);
    await until(async () => !(await api.storage.session.get('site-1'))['site-1']);
  } finally {
    held.resolve();
    await until(async () => !(await success('status')).running);
    await success('logout', { pubkey: owner });
  }
  // Signed out, X shows a yellow mark that asks to sign in.
  await success('captureConfig', {}, x);
  await until(() => badges.get(1)?.title === 'Social Backup: Sign in to back up your posts');
  assert.equal(badges.get(1)?.path?.[16], 'icon-attention-16.png');
});

test('a relay that never answers or refuses the connection fails alone', async () => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const [hanging, refused, working] = ['wss://hanging.example/', 'wss://refused.example/', 'wss://working.example/'];
  fixtureRelay(hanging).connect = 'hang';
  fixtureRelay(refused).connect = 'refuse';
  const healthy = fixtureRelay(working);
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: '', capture: false, relaySync: false, relays: [hanging, refused, working].join(' ') });
    await idle().catch(() => {});
    const db = await openArchive('nostr-backup-' + owner);
    const event = finalizeEvent({ kind: 1, created_at: 30, tags: [], content: 'Reaches the working relay' }, secret);
    await append(db, [{ event }], owner);
    db.close();
    await success('broadcast');
    const relay = async (url: string) => (await success('status')).relays.find((r) => r.url === url)!;
    // The working relay is not held up by the one still connecting.
    await until(async () => (await relay(working)).phase === 'finished');
    assert.deepEqual(healthy.sent.map((e) => e.id), [event.id]);
    assert.equal((await relay(hanging)).phase, 'connecting');
    // Each failure stays with its own relay once the connection limit passes.
    for (let i = 0; i < 100 && (await success('status')).running; i++) await delay(100);
    const [late, refusing, done] = await Promise.all([hanging, refused, working].map(relay));
    assert.deepEqual([late.phase, late.detail, late.pending], ['error', 'connection timed out', 1]);
    assert.deepEqual([refusing.phase, refusing.detail, refusing.pending], ['error', 'connection failed', 1]);
    assert.deepEqual([done.phase, done.sent, done.pending], ['finished', 1, 0]);
  } finally {
    await success('logout', { pubkey: owner });
  }
});

test('new X captures reach a relay before its history import finishes', async (t) => {
  t.mock.method(globalThis, 'fetch', (url: URL) =>
    publicTweet(url, 'Post ' + String(url).split('/').pop()));
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const url = 'wss://slow-history.example/';
  const relay = fixtureRelay(url);
  const reads = Promise.withResolvers<void>();
  relay.holdRead = reads.promise;
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: true, relays: url });
    await success('capture', { posts: [{ author: 'alice', id: '1', time: 1700000000, text: 'Post 1' }] }, x);
    await until(() => relay.sent.some(event => event.content === 'Post 1'));
    assert.equal((await success('status')).pending, 0);
  } finally {
    reads.resolve();
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('a truncated X post is backed up with its full public text', async (t) => {
  const full = 'The opening paragraph.\n\nThe rest of a long post that X hides behind Show more.';
  const page = 'https://x.com/alice/status/501';
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    const url = String(input);
    assert.equal(url, page);
    const data = { data: { tweet_result_by_rest_id: { result: {
      __typename: 'Tweet', rest_id: '501',
      core: { user_results: { result: { core: { screen_name: 'alice' }, privacy: { protected: false } } } },
      details: { full_text: full, created_at_ms: 1700000000000 }, media_entities2: [],
    } } } };
    const response = new Response('<script>const data=' + JSON.stringify(JSON.stringify(data)) + '</script>',
      { headers: { 'Content-Type': 'text/html' } });
    Object.defineProperty(response, 'url', { value: page });
    return response;
  });
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const url = 'wss://long-post.example/';
  const relay = fixtureRelay(url);
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url });
    await success('capture', { posts: [{ author: 'alice', id: '501', time: 1700000000, text: 'The opening paragraph.', truncated: true }] }, x);
    await until(() => relay.sent.length === 1);
    assert.equal(relay.sent[0].content, full);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('sources save switches at once but keep a typed username unsaved until Save', async (t) => {
  const url = 'wss://sources-page.example/';
  fixtureRelay(url);
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url });
    const page = await archivePage(t), id = pageElements(page);
    const conf = async () => (await success('status')).conf;
    const leaveBlocked = () => {
      const event = new page.Event('beforeunload', { cancelable: true });
      page.dispatchEvent(event);
      return event.defaultPrevented;
    };
    const type = (value: string) => {
      id('handle').value = value;
      page.document.getElementById('handle')!.dispatchEvent(new page.Event('input', { bubbles: true }));
    };
    await until(() => id('instagramCapture').checked && !id('capture').disabled);
    assert.equal(leaveBlocked(), false);
    type('@Alice');
    await until(() => page.document.querySelector('.unsaved')?.textContent === 'Unsaved changes');
    assert.equal(leaveBlocked(), true);
    // A switch saves right away but never sends the unsaved username.
    id('instagramCapture').click();
    await until(async () => (await conf()).social?.instagram?.capture === false);
    assert.equal((await conf()).handle, '');
    assert.equal(id('handle').value, '@Alice');
    assert.ok(page.document.querySelector('.unsaved'));
    id('handleSave').click();
    await until(async () => (await conf()).handle === 'alice');
    await until(() => !page.document.querySelector('.unsaved'));
    assert.equal(page.document.getElementById('handleSave'), null);
    assert.equal(leaveBlocked(), false);
    // A rejected username stays in the field, marked unsaved, and the stored one is kept.
    type('not valid!');
    await until(() => !!page.document.getElementById('handleSave'));
    id('handleSave').click();
    await until(() => id('notice').textContent === 'Enter your X handle.');
    assert.equal((await conf()).handle, 'alice');
    assert.equal(id('handle').value, 'not valid!');
    assert.ok(page.document.querySelector('.unsaved'));
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('Sources offers differing signed-in accounts on demand and stages a selection until Save', async (t) => {
  const secret = generateSecretKey(), owner = getPublicKey(secret), relay = 'wss://suggestions.example/';
  fixtureRelay(relay);
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'alice', relaySync: false, relays: relay,
      social: { facebook: { handle: 'old.name', capture: true } } });
    const queried: string[][] = [];
    const later = Promise.withResolvers<void>();
    t.after(() => later.resolve());
    t.mock.method(api.tabs, 'query', async ({url}: {url: string[]}) => {
      queried.push(url);
      return [{id: url[0].includes('facebook') ? 2 : url[0].includes('instagram') ? 3 : 1}];
    });
    t.mock.method(api.scripting, 'executeScript', async ({target, args}: Injection) => {
      if (target.tabId === 1) return [{result: '/Alice'}];
      if (target.tabId === 2) return [{result: '123456789'}];
      assert.deepEqual(args, [false]);
      await later.promise;
      return [{result: {username: 'alice.ig'}}];
    });
    const page = await archivePage(t), id = pageElements(page);
    await until(() => id('handle').value === 'alice');
    assert.equal(queried.length, 0);
    page.location.hash = 'sources';
    page.dispatchEvent(new page.Event('hashchange'));
    await until(() => !!page.document.getElementById('facebookHandleSuggestion'));
    assert.equal(page.document.getElementById('handleSuggestion'), null);
    assert.equal(id('facebookHandle').value, 'old.name');
    // A slow reply never replaces a username being typed.
    id('instagramHandle').value = 'manual';
    id('instagramHandle').dispatchEvent(new page.Event('input', { bubbles: true }) as unknown as Event);
    later.resolve();
    await until(() => !!page.document.getElementById('instagramHandleSave'));
    id('facebookHandleSuggestion').click();
    await until(() => id('facebookHandle').value === '123456789');
    assert.equal((await success('status')).conf.social?.facebook?.handle, 'old.name');
    id('facebookHandleSave').click();
    await until(async () => (await success('status')).conf.social?.facebook?.handle === '123456789');
    assert.equal(page.document.getElementById('facebookHandleSuggestion'), null);
    assert.equal(page.document.getElementById('instagramHandleSuggestion'), null);
    assert.equal(id('instagramHandle').value, 'manual');
    assert.equal((await success('status')).conf.social?.instagram?.handle, '');
    assert.equal((await call('suggestAccount', {source: 'facebook'}, x)).ok, false);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('X linking, follow lookup and following on Nostr are separate and keep existing follows', async (t) => {
  const { proofTweet } = await import('../src/x-identity.ts');
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const bob = generateSecretKey(), impostor = generateSecretKey(), friend = getPublicKey(generateSecretKey());
  const url = 'wss://identity.example/';
  const relay = fixtureRelay(url);
  const store = (event: NostrEvent) => relay.stored.set(event.id, event);
  // Bob proved his X account; someone else copied his proof tweet ID.
  for (const key of [bob, impostor])
    store(finalizeEvent({ kind: 10011, created_at: 5, content: '', tags: [['i', 'twitter:bob', '77']] }, key));
  store(finalizeEvent({ kind: 3, created_at: 9, content: '', tags: [['p', friend]] }, secret));
  const tweets: Record<string, string> = {
    '501': proofTweet(owner),
    '502': proofTweet(owner),
    '77': 'Verifying my account on nostr My Public Key: ' + getPublicKey(bob),
  };
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) =>
    publicTweet(input, tweets[String(input).split('/').pop()!] ?? 'Hello'));
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url });
    // Publishing your own proof happens only when you post a verification tweet.
    await success('capture', { posts: [{ author: 'alice', id: '500', time: 1700000000, text: 'My nostr is ' + npubEncode(owner) }] }, x);
    await idle();
    assert.equal((await success('status')).xLink, undefined);
    await success('capture', { posts: [{ author: 'alice', id: '501', time: 1700000001, text: proofTweet(owner) }] }, x);
    await until(() => relay.sent.some((e) => e.kind === 10011));
    assert.deepEqual(relay.sent.find((e) => e.kind === 10011)!.tags, [['i', 'twitter:alice', '501'], ['client', 'Social Backup']]);
    assert.deepEqual((await success('status')).xLink, { handle: 'alice', proof: '501' });
    // Your following list is read only from your own following page.
    const following = { ...x, url: 'https://x.com/alice/following', tab: { id: 1, url: 'https://x.com/alice/following' } };
    assert.equal((await call('xFollows', { handles: ['bob'] }, x)).ok, false);
    await success('xFollows', { handles: ['bob', 'carol'] }, { ...following, url: x.url });
    assert.equal((await call('xFollows', { handles: ['dave'] }, { ...following, tab: x.tab })).ok, false);
    assert.equal((await success('status')).xFollows, 2);
    await success('xFollows', { handles: [], total: 1234 }, x);
    assert.equal((await success('status')).xFollowingTotal, 1234);
    assert.equal((await call('xFollows', { handles: [], total: -1 }, x)).ok, false);
    await success('settings', { pubkey: owner, handle: 'different', capture: false, relaySync: false, relays: url });
    assert.equal((await success('status')).xFollows, 0);
    assert.equal((await success('status')).xFollowingTotal, undefined);
    await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url });
    assert.equal((await success('status')).xFollows, 2);
    assert.equal((await success('status')).xFollowingTotal, 1234);
    // Looking people up checks each proof: the impostor fails.
    // Refresh starts filling the directory again without clearing the local matches.
    await success('findXFollows', { refresh: true });
    let found = await success('findXFollows');
    await until(async () => !(found = await success('findXFollows')).loading && found.matches.length > 0);
    assert.deepEqual(found.matches.map((m) => m.pubkey), [getPublicKey(bob)]);
    assert.equal(found.unchecked, 0);
    assert.deepEqual(found.following, []);
    // A follow made in another Nostr client is reflected without waiting for archive sync.
    store(finalizeEvent({ kind: 3, created_at: 10, content: '', tags: [['p', friend], ['p', getPublicKey(bob)]] }, secret));
    assert.deepEqual((await success('findXFollows')).following, [getPublicKey(bob)]);
    await success('followOnNostr', { pubkeys: [getPublicKey(bob)] });
    await until(() => relay.sent.some((e) => e.kind === 3));
    assert.deepEqual(relay.sent.find((e) => e.kind === 3)!.tags, [['p', friend], ['p', getPublicKey(bob)]]);
    // Deleting the X proof removes the match on the next search.
    const bobProof = tweets['77'];
    delete tweets['77'];
    assert.equal((await success('findXFollows', { refresh: true })).matches.length, 0);
    tweets['77'] = bobProof;
    assert.equal((await success('findXFollows')).matches.length, 1);
    // A newer signed identity list that no longer lists X revokes it too.
    store(finalizeEvent({ kind: 10011, created_at: 6, content: '', tags: [['i', 'instagram:bob', 'other-proof']] }, bob));
    assert.equal((await success('findXFollows', { refresh: true })).matches.length, 0);
    await success('unlinkX');
    await until(() => relay.sent.some(e => e.kind === 10011 && !e.tags.some(([t]) => t === 'i')));
    assert.equal((await success('status')).xLink, undefined);
    await success('capture', { posts: [{ author: 'alice', id: '501', time: 1700000001, text: proofTweet(owner) }] }, x);
    assert.equal((await success('status')).xLink, undefined);
    // A new proof also links from any X page or a pasted link, once X's public page confirms it.
    const home = { ...x, url: 'https://x.com/home', tab: { id: 1, url: 'https://x.com/home' } };
    assert.equal((await success('captureConfig', {}, home)).xLinked, false);
    assert.equal((await call('xProof', { id: '500' }, home)).ok, false);
    const wrongAuthor = await call('xProof', { url: 'https://x.com/bob/status/77' });
    assert.ok(!wrongAuthor.ok && /from @bob/.test(wrongAuthor.error));
    await success('xProof', { url: 'https://x.com/alice/status/502?s=20' });
    assert.deepEqual((await success('status')).xLink, { handle: 'alice', proof: '502' });
    assert.equal((await success('captureConfig', {}, home)).xLinked, true);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('signing in with an existing key keeps its published X link and profile', async (t) => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  // A new device starts with the default relays, where the existing account published.
  const url = DEFAULT_RELAYS[0];
  const relay = fixtureRelay(url);
  const profile = finalizeEvent({ kind: 0, created_at: 3, content: JSON.stringify({ name: 'Existing' }), tags: [] }, secret);
  for (const event of [
    finalizeEvent({ kind: 10011, created_at: 5, content: '', tags: [['i', 'github:existing', 'gist'], ['i', 'twitter:existing', '321']] }, secret),
    profile,
  ]) relay.stored.set(event.id, event);
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) =>
    publicTweet(input, 'Verifying that I control the following Nostr public key: ' + npubEncode(owner)));
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: 'existing', capture: true, relaySync: false, relays: url });
    // The proof is checked on X, then kept as this account's link without publishing anything.
    await until(async () => (await success('status')).xLink?.proof === '321');
    assert.deepEqual((await success('status')).xLink, { handle: 'existing', proof: '321' });
    assert.equal(relay.sent.filter((event) => event.kind === 0 || event.kind === 10011).length, 0);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('wallet access, automatic inbox checks and public address edits follow account settings', async (t) => {
  const previous = await success('status');
  if (previous.conf.pubkey) await success('logout', { pubkey: previous.conf.pubkey });
  const key = generateSecretKey(), owner = getPublicKey(key);
  const walletPage = { ...ui, url: ext + 'wallet.html' };
  const account = { account: owner };
  const relay = 'wss://wallet-profile.example/';
  const original = finalizeEvent({kind:0, created_at:100, tags:[['custom','tag']],
    content:JSON.stringify({name:'Receiver', extra:{color:'green'}})}, key);
  fixtureRelay(relay).stored.set(original.id, original);
  let inboxCalls = 0;
  const oldFetch = globalThis.fetch;
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    if (!String(input).startsWith('https://npub.cash/')) return oldFetch(input, init);
    inboxCalls++;
    return Response.json({error:false,data:{quotes:[]},metadata:{total:0}});
  });
  await success('login', {key: nsecEncode(key)});
  try {
    await success('settings', {pubkey:owner, handle:'',capture:false,relaySync:false,relays:relay});
    assert.deepEqual(await success('walletIdentity', {}, walletPage), {pubkey:owner, unlocked:true, enabled:false, autoReceive:false});
    assert.equal((await call('walletPreferences', {...account, enabled:true, autoReceive:true}, x)).ok, false);
    assert.equal((await call('walletStatus', account, walletPage)).ok, false);
    assert.equal((await call('walletPreferences', {enabled:true,autoReceive:false}, walletPage)).ok, false);
    await success('walletPreferences', {...account,enabled:true,autoReceive:false},walletPage);
    assert.equal((await success('walletStatus',account,walletPage)).balance, 0);
    const popup = {...ui, url: ext + 'popup.html'};
    assert.equal(await success('tipBalance', account, popup), 0);
    assert.equal((await call('tipBalance', {}, popup)).ok, false);
    assert.equal((await call('tipBalance', {}, x)).ok, false);
    await success('settings', {pubkey:owner, handle:'',capture:false,relaySync:false,relays:relay,tipButtons:true});
    assert.equal(await success('tipBalance', {}, x), 0);
    assert.equal((await call('walletStatus', account, x)).ok, false);
    await success('openTipWallet', {}, x);
    assert.equal(tabURLs.at(-1), ext + 'wallet.html');
    assert.equal((await call('openTipWallet', { invoice: 'invalid' }, x)).ok, false);
    api.alarms.onAlarm.list[0]({name:'cashu-wallet'});
    await success('walletStatus', account, walletPage);
    assert.equal(inboxCalls,0);
    await success('walletPreferences',{...account,enabled:true,autoReceive:true},walletPage);
    api.alarms.onAlarm.list[0]({name:'cashu-wallet'});
    await until(()=>inboxCalls === 1);
    assert.equal((await success('walletIdentity',{},walletPage)).enabled,true);
    await success('walletPreferences',{...account,enabled:false,autoReceive:false},walletPage);
    assert.equal((await call('tipBalance', account, popup)).ok, false);
    assert.equal((await call('tipBalance', {}, x)).ok, false);
    assert.equal((await call('openTipWallet', {}, x)).ok, false);
    assert.equal((await call('walletIncoming',account,walletPage)).ok,false);
    assert.equal((await call('walletConfirm',{...account,id:'no-payment',maxFee:0},x)).ok,false);
    assert.equal((await call('walletExport',{...account,passphrase:'test recovery password'},x)).ok,false);
    assert.equal((await call('receivingAddress',{...account,enabled:true},x)).ok,false);
    await success('receivingAddress',{...account,enabled:true});
    assert.equal((await success('status')).profile?.lightning,npubEncode(owner)+'@npub.cash');
    await success('receivingAddress',{...account,enabled:false});
    const db = await openArchive((await success('status')).conf.archiveName);
    try {
      const latest = (await db.getAll('events')).filter(e=>e.kind===0).sort((a,b)=>b.created_at-a.created_at)[0];
      assert.deepEqual(JSON.parse(latest.content),JSON.parse(original.content));
      assert.deepEqual(latest.tags,original.tags);
    } finally { db.close(); }
    await success('lock');
    assert.equal((await call('walletPreferences',{...account,enabled:true,autoReceive:true},walletPage)).ok,false);
  } finally { await success('logout',{pubkey:owner}); }
});

test('one-click tips verify the active post, bind each approval to its wallet window and retain the issued intent', async t => {
  const secret = generateSecretKey(), owner = getPublicKey(secret), recipientKey = generateSecretKey(), pubkey = getPublicKey(recipientKey);
  const relayURL = 'wss://one-click.example/', relay = fixtureRelay(relayURL);
  const addr = 'alice@quick.example', handle = 'alice', proof = '81', post = '43', sats = 21;
  const profile = finalizeEvent({kind:0, created_at:12, content:JSON.stringify({lud16:addr}), tags:[]}, recipientKey);
  const identity = finalizeEvent({kind:10011, created_at:10, content:'', tags:[['i','twitter:alice',proof]]}, recipientKey);
  for (const ev of [profile,identity]) relay.stored.set(ev.id,ev);
  const page = {...x, url:'https://x.com/home', tab:{id:9,url:'https://x.com/home'}};
  const settingsPage = {...ui, url:ext+'wallet.html'};
  const params = {account:owner, handle, pubkey, post, sats};
  let nextWindow = 21, failOpen = false, active = page.tab.id;
  const windows: {url:string; id:number}[] = [];
  t.mock.method(api.tabs, 'get', async () => ({url:page.url}));
  t.mock.method(api.tabs, 'query', async (options: {active?:boolean}) => options.active ? [{id:active,url:page.url}] : []);
  t.mock.method(api.windows,'create',async (opts:{url:string}) => {
    if (failOpen) throw Error('Popup blocked');
    const win = {...opts,id:nextWindow++}; windows.push(win); return win;
  });
  const endpoint = process.env.SOCIAL_BACKUP_FAKE_MINT;
  if (endpoint) assert.equal(new URL(endpoint).hostname,'127.0.0.1');
  const nativeFetch = fetch;
  let invoiceCount = 0, melts = 0, postValid = true, proofValid = true, hideDuringLookup = false;
  let invoice = '';
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    if (url.origin === 'https://x.com') {
      if (url.pathname.endsWith('/'+proof)) return publicTweet(url,proofValid ? 'Verifying my account on nostr My Public Key: ' + pubkey : 'Withdrawn proof');
      const requestedPost = url.pathname.split('/').at(-1)!;
      const response = new Response(publicTweetHTML(postValid ? handle : 'impostor',requestedPost,'A public post',1700000000),
        {headers:{'Content-Type':'text/html'}});
      Object.defineProperty(response, 'url', {value:url.href});
      if (hideDuringLookup) active = 102;
      return response;
    }
    if (url.hostname === 'quick-test-mint.example' && endpoint) {
      if (url.pathname === '/v1/melt/bolt11' && init?.method === 'POST') melts++;
      return nativeFetch(endpoint + url.pathname,init);
    }
    if (url.pathname === '/.well-known/lnurlp/alice')
      return Response.json({tag:'payRequest',callback:'https://quick.example/pay',minSendable:1000,maxSendable:1e9});
    if (url.pathname === '/pay') {
      const amount = Number(url.searchParams.get('amount')) / 1000;
      invoiceCount++;
      invoice = endpoint ? (await (await nativeFetch(endpoint+'/v1/mint/quote/bolt11', {method:'POST',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify({amount,unit:'sat'})})).json()).request
        : signedInvoice(amount,'quick invoice '+invoiceCount,Math.floor(Date.now()/1000),true);
      return Response.json({pr:invoice, verify:'https://quick.example/verify'});
    }
    if (url.pathname === '/verify') return Response.json({status:'OK',settled:false,pr:invoice,preimage:null});
    throw Error('Unexpected quick tip test request');
  });
  const open = (args = params, sender = page) => success('quickTip',args,sender);
  const latest = () => {
    const win = windows.at(-1)!;
    const [id,token] = new URL(win.url).hash.slice(1).split('?quick=');
    assert.match(token,/^[a-f0-9]{32}$/);
    return {id, token, sender:{...settingsPage, url:win.url, tab:{id:90,windowId:win.id}}};
  };
  await success('login',{key:nsecEncode(secret)});
  try {
    await success('settings',{pubkey:owner,handle:'',capture:false,relaySync:false,relays:relayURL,tipButtons:true});
    const archive = await openArchive((await success('status')).conf.archiveName);
    await (await import('../src/x-directory.ts')).saveDirectory(archive,[identity,profile]);
    archive.close();
    assert.equal((await call('quickTip',params,page)).ok,false);
    assert.equal((await call('walletSetQuickTip',{account:owner,sats},page)).ok,false);
    await success('walletPreferences',{account:owner,enabled:true,autoReceive:false},settingsPage);
    for (const invalid of [-1,0.5,1000001]) assert.equal((await call('walletSetQuickTip',{account:owner,sats:invalid},settingsPage)).ok,false);
    await success('walletSetQuickTip',{account:owner,sats},settingsPage);
    assert.deepEqual((await success('captureConfig',{},page)).quickTip,{account:owner,sats});
    assert.equal((await success('captureConfig',{}, {...page,url:'https://www.instagram.com/alice/'})).quickTip,undefined);
    assert.equal((await call('quickTip',{...params,sats:sats+1},page)).ok,false);
    assert.equal((await call('quickTip',{...params,account:pubkey},page)).ok,false);
    assert.equal((await call('quickTip',params,{...page,frameId:1})).ok,false);
    assert.equal((await call('quickTip',params,{...page,url:'https://example.org/'})).ok,false);
    await success('lock');
    assert.equal((await success('captureConfig',{},page)).quickTip,undefined);
    assert.equal((await call('quickTip',params,page)).ok,false);
    await success('unlock',{key:nsecEncode(secret)});
    postValid=false;
    assert.equal((await call('quickTip',params,page)).ok,false);
    postValid=true; proofValid=false;
    assert.equal((await call('quickTip',params,page)).ok,false);
    proofValid=true;
    assert.equal((await call('quickTip',{...params,pubkey:getPublicKey(generateSecretKey())},page)).ok,false);
    assert.equal(invoiceCount,0);
    hideDuringLookup=true;
    assert.equal((await call('quickTip',params,page)).ok,false);
    assert.equal(windows.length,0);
    hideDuringLookup=false; active=page.tab.id;
    failOpen=true;
    assert.equal((await call('quickTip',params,page)).ok,false);
    assert.equal(melts,0);
    failOpen=false;
    assert.deepEqual(await open(),{state:'opened'});
    const first = latest();
    // A second live tab opens the same review and cannot create a separate invoice.
    active=10;
    assert.deepEqual(await open(params,{...page,tab:{id:active,url:page.url}}),{state:'opened'});
    active=page.tab.id;
    assert.equal(windows.length,1);
    assert.equal(invoiceCount,1);
    const request = {account:owner,id:first.id,token:first.token,maxFee:0};
    assert.equal((await call('walletQuickConfirm',request,page)).ok,false);
    assert.equal((await call('walletQuickConfirm',request,{...first.sender,tab:{id:3,windowId:120}})).ok,false);
    assert.equal((await call('walletQuickConfirm',{...request,token:'0'.repeat(32)},first.sender)).ok,false);
    const expensive = await call('walletQuickConfirm',{...request,maxFee:3},first.sender);
    assert.ok(!expensive.ok && /fee exceeds/.test(expensive.error));
    const replay = await call('walletQuickConfirm',request,first.sender);
    assert.ok(!replay.ok && /Review this tip/.test(replay.error));
    await open();
    const stale = latest();
    const clockNow = Date.now.bind(Date);
    const clock = t.mock.method(Date,'now',() => clockNow()+61_000);
    try {
      const late = await call('walletQuickConfirm',{...request,token:stale.token},stale.sender);
      assert.ok(!late.ok && /approval expired/.test(late.error));
    } finally { clock.mock.restore(); }
    await open();
    const beforeDisable=latest();
    await success('walletPreferences',{account:owner,enabled:false,autoReceive:false},settingsPage);
    assert.equal((await success('captureConfig',{},page)).quickTip,undefined);
    await success('walletPreferences',{account:owner,enabled:true,autoReceive:false},settingsPage);
    assert.equal((await success('walletIdentity',{},settingsPage)).quickTipSats,undefined);
    await success('walletSetQuickTip',{account:owner,sats},settingsPage);
    assert.equal((await call('walletQuickConfirm',{...request,token:beforeDisable.token},beforeDisable.sender)).ok,false);
    assert.equal(melts,0);
    if (endpoint) {
      await success('walletSelectMint',{account:owner,url:'https://quick-test-mint.example'},settingsPage);
      const funding = await success('walletFund',{account:owner,sats:23},settingsPage);
      const deadline = Date.now()+35000;
      while ((await success('walletRefreshFunding',{account:owner,id:funding.id},settingsPage)).state !== 'paid') {
        assert.ok(Date.now()<deadline,'test mint funding timeout'); await delay(175);
      }
      // Fresh recipient invoice after funding the wallet; the fake back end auto-pays its own quotes.
      const newPost={...params,post:'44'};
      await open(newPost);
      const live=latest();
      const reviewed = await success('walletPrepare',{account:owner,id:live.id},live.sender);
      assert.ok(reviewed.fee<=2, 'test fixture fee stays under the one-click ceiling');
      const hidden = await call('walletQuickConfirm',{account:owner,id:live.id,token:live.token,maxFee:reviewed.fee},live.sender);
      assert.ok(!hidden.ok && /no longer active/.test(hidden.error));
      assert.equal(melts,0);
      await open(newPost);
      const visible = latest();
      assert.equal(visible.id,live.id);
      active=visible.sender.tab.id;
      const pay = {account:owner,id:visible.id,token:visible.token,maxFee:reviewed.fee};
      const paid=await success('walletQuickConfirm',pay,visible.sender);
      assert.equal((await call('walletQuickConfirm',pay,visible.sender)).ok,false);
      active=page.tab.id;
      if (paid.state === 'pending') await until(async () => (await success('walletRefresh',{account:owner,id:live.id},live.sender)).state === 'paid');
      else assert.equal(paid.state,'paid');
      assert.equal(melts,1);
      assert.deepEqual(await open(newPost),{state:'already'});
      assert.deepEqual(await success('tipMarks',{targets:[{post:newPost.post,pubkey}]},page),[sats]);
      assert.equal(melts,1);
    }
    await success('walletSetQuickTip',{account:owner,sats:0},settingsPage);
    assert.equal((await success('captureConfig',{},page)).quickTip,undefined);
  } finally { await success('logout',{pubkey:owner}); }
});

test('tips on X go to verified Nostr profiles with a lightning address and can be turned off', async (t) => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const carol = generateSecretKey(), carolKey = getPublicKey(carol);
  const url = 'wss://tips.example/';
  const relay = fixtureRelay(url);
  const provider = generateSecretKey();
  const copy = finalizeEvent({ kind: 1, created_at: 1700000000, content: 'Hello', tags: [['i', 'https://x.com/carol/status/42']] }, carol);
  const receipt = (key: Uint8Array) => signedZap(carol, key, carolKey, 21, copy.id).receipt;
  let issuedInvoice = '', settledExternally = false, verificationReads = 0;
  for (const event of [
    finalizeEvent({ kind: 10011, created_at: 5, content: '', tags: [['i', 'twitter:carol', '88']] }, carol),
    finalizeEvent({ kind: 0, created_at: 5, content: JSON.stringify({ name: 'Carol', lud16: 'carol@wallet.example' }), tags: [] }, carol),
    finalizeEvent({ kind: 3, created_at: 4, content: '', tags: [['p', carolKey]] }, secret),
    copy, receipt(provider), receipt(generateSecretKey()),
  ]) relay.stored.set(event.id, event);
  let proofActive = true;
  const zaps: Record<string, unknown>[] = [];
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    const target = new URL(String(input));
    if (target.origin === 'https://x.com') return publicTweet(input, proofActive ? 'Verifying my account on nostr My Public Key: ' + carolKey : '');
    if (target.pathname === '/.well-known/lnurlp/carol')
      return Response.json({ tag: 'payRequest', callback: 'https://wallet.example/pay', minSendable: 1000, maxSendable: 1e9, allowsNostr: true, nostrPubkey: getPublicKey(provider) });
    if (target.pathname === '/pay') {
      zaps.push(JSON.parse(target.searchParams.get('nostr')!));
      issuedInvoice = signedInvoice(100, target.searchParams.get('nostr')!);
      return Response.json({ pr: issuedInvoice, verify: 'https://wallet.example/verify' });
    }
    if (target.pathname === '/verify') {
      verificationReads++;
      return Response.json({ status: 'OK', settled: settledExternally, pr: issuedInvoice, preimage: null });
    }
    if (target.hostname === 'api.coinbase.com') return Response.json({ data: { currency: 'BTC', rates: { EUR: '90000' } } });
    throw Error('Unexpected fetch ' + target.href);
  });
  const page = { ...x, url: 'https://x.com/home', tab: { id: 1, url: 'https://x.com/home' } };
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url });
    assert.equal((await success('captureConfig', {}, page)).tips, false);
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url, tipButtons: true });
    assert.equal((await success('captureConfig', {}, page)).tips, true);
    // Amounts follow the browser's region until a currency is chosen.
    assert.equal((await success('captureConfig', {}, page)).tipCurrency, undefined);
    assert.equal((await call('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url, tipCurrency: 'XYZ' })).ok, false);
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url, tipCurrency: 'JPY' });
    assert.equal((await success('captureConfig', {}, page)).tipCurrency, 'JPY');
    assert.equal((await success('status')).conf.tipButtons, true);
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url, tipCurrency: '' });
    assert.equal((await success('captureConfig', {}, page)).tipCurrency, undefined);
    api.alarms.onAlarm.list[0]({ name: 'x-directory' });
    await until(() => relay.reads.some(filter => filter.kinds?.includes(10011) && filter.authors?.includes(carolKey)));
    await idle();
    const dir = await openArchive('nostr-backup-' + owner);
    await until(async () => !!await dir.get('meta', ['xDirectorySchedule', url]));
    // A fresh pass compares signed IDs for graph authors, independently of page activity.
    await dir.delete('meta', ['xDirectorySchedule', url]);
    api.alarms.onAlarm.list[0]({ name: 'x-directory' });
    await until(async () => !!await dir.get('meta', ['xDirectorySchedule', url]));
    assert.ok(relay.filters.some(filter => filter.kinds?.includes(10011) && filter.authors?.includes(carolKey)));
    dir.close();
    const reads = relay.reads.length;
    const targets = await success('tipTargets', { handles: ['carol', 'dave'] }, page);
    assert.equal(relay.reads.length, reads);
    assert.deepEqual({ ...targets }, { carol: { pubkey: carolKey, proof: '88' }, dave: null });
    const recipient = await success('tipProfile', { handle: 'carol' }, page);
    assert.equal(recipient.lightning, 'carol@wallet.example');
    // Opening the popover finds the post's Nostr copy; only the provider's receipt counts.
    assert.deepEqual(await success('tipPost', { handle: 'carol', post: '42' }, page), { note: copy.id, sats: 21, tips: 1, ownSats: 0, ownTips: 0 });
    // 100 sats is 1u in an invoice. The zap goes to the post's note.
    assert.deepEqual(await success('tipInvoice', { ...recipient, handle: 'carol', sats: 100, post: '42' }, page), { invoice: issuedInvoice, kind: 'zap' });
    assert.equal(zaps[0].pubkey, owner);
    assert.deepEqual((zaps[0].tags as string[][]).find(([t]) => t === 'e'), ['e', copy.id]);
    assert.equal(zaps[0].content, '');
    // Closing the invoice and reopening the menu still recovers a payment from an external wallet.
    assert.equal((await success('tipPost', { handle: 'carol', post: '42', invoice: issuedInvoice }, page)).paid, false);
    settledExternally = true;
    const history = await success('tipPost', { handle: 'carol', post: '42' }, page);
    assert.equal(history.ownSats, 100);
    assert.equal(history.sats, 121);
    const beforeMarks = [verificationReads, relay.reads.length];
    assert.deepEqual(await success('tipMarks', {targets: [{pubkey: carolKey, post: '42'}, {pubkey: carolKey}]}, page), [100, 0]);
    assert.deepEqual([verificationReads, relay.reads.length], beforeMarks);
    const updated = finalizeEvent({ kind: 0, created_at: 6, content: '{"lud16":"other@wallet.example"}', tags: [] }, carol);
    relay.stored.set(updated.id, updated);
    const changed = await call('tipInvoice', { ...recipient, handle: 'carol', sats: 100 }, page);
    assert.ok(!changed.ok && /recipient changed/.test(changed.error));
    proofActive = false;
    assert.equal((await call('tipProfile', { handle: 'carol' }, page)).ok, false);
    assert.equal((await call('tipInvoice', { ...recipient, handle: 'carol', sats: 100 }, page)).ok, false);
    assert.equal(zaps.length, 1);
    assert.doesNotMatch(JSON.stringify([relay.filters, relay.reads, zaps]), /twitter:|x\.com|carol|dave/i);
    assert.equal((await call('tipInvoice', { ...recipient, handle: 'dave', sats: 100 }, page)).ok, false);
    assert.equal(await success('tipRate', { currency: 'EUR' }, page), 90000);
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url, tipButtons: false });
    assert.equal((await success('captureConfig', {}, page)).tips, false);
    const beforeLocal = [verificationReads, relay.reads.length];
    assert.equal((await call('tipTargets', { handles: ['carol'] }, page)).ok, true);
    assert.deepEqual([verificationReads, relay.reads.length], beforeLocal);
    assert.equal((await call('tipProfile', { handle: 'carol' }, page)).ok, false);
    assert.equal((await call('tipInvoice', { ...recipient, handle: 'carol', sats: 100 }, page)).ok, false);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('the popup card shows a verified X account on Nostr with known followers, with tips off', async (t) => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const carol = generateSecretKey(), carolKey = getPublicKey(carol);
  const url = 'wss://card.example/';
  const relay = fixtureRelay(url);
  for (const event of [
    finalizeEvent({ kind: 10011, created_at: 5, content: '', tags: [['i', 'twitter:carol', '88']] }, carol),
    finalizeEvent({ kind: 0, created_at: 5, content: JSON.stringify({ name: 'carol', display_name: 'Carol', about: 'Gardener.\nWrites about bees.' }), tags: [] }, carol),
    finalizeEvent({ kind: 3, created_at: 4, content: '', tags: [['p', carolKey]] }, secret),
  ]) relay.stored.set(event.id, event);
  // A one-row follower table for Carol: her key's first 10 bytes, then 7.
  const table = new Uint8Array(12);
  table.set(Buffer.from(carolKey.slice(0, 20), 'hex'));
  table[11] = 7;
  const fetched: string[] = [];
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    fetched.push(String(input));
    if (String(input).endsWith('graph-followers.bin')) return new Response(table);
    return publicTweet(input, 'Verifying my account on nostr My Public Key: ' + carolKey);
  });
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: '', capture: true, relaySync: false, relays: url });
    // The public directory refreshes without tips, so the card works when tips are off.
    api.alarms.onAlarm.list[0]({ name: 'x-directory' });
    await until(() => relay.reads.some(filter => filter.kinds?.includes(10011) && filter.authors?.includes(carolKey)));
    await idle();
    const dir = await openArchive('nostr-backup-' + owner);
    await until(async () => !!await dir.get('meta', ['xDirectorySchedule', url]));
    dir.close();
    // A profile without a directory claim makes no requests.
    const reads = relay.reads.length, fetches = fetched.length;
    assert.equal(await success('nostrCard', { handle: 'dave' }), null);
    assert.equal(relay.reads.length, reads);
    assert.equal(fetched.length, fetches);
    // The card only checks the proof on X: relays never learn which X profile you are looking at.
    const before = relay.reads.length;
    assert.deepEqual(await success('nostrCard', { handle: 'Carol' }), {
      handle: 'carol',
      pubkey: carolKey,
      profile: { name: 'carol', displayName: 'Carol', picture: undefined, about: 'Gardener.\nWrites about bees.' },
      followers: 7,
      following: true, // The independent directory refresh observed our signed follow list.
    });
    assert.equal(relay.reads.length, before);
    // Following is an intentional public action, so it reads your follow list from relays first.
    await success('followOnNostr', { pubkeys: [carolKey] });
    assert.equal((await success('nostrCard', { handle: 'carol' }))?.following, true);
    // X pages cannot ask for cards.
    assert.equal((await call('nostrCard', { handle: 'carol' }, x)).ok, false);
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('waiting to send counts posts for the current relays, not one row per relay', async (t) => {
  t.mock.method(globalThis, 'fetch', (url: URL) =>
    publicTweet(url, 'Post ' + String(url).split('/').pop()));
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const a = 'wss://count-a.example/', b = 'wss://count-b.example/', gone = 'wss://count-gone.example/';
  const relays = [a, b, gone].map(url => fixtureRelay(url));
  const held = Promise.withResolvers<void>();
  for (const relay of relays) relay.holdPublish = held.promise;
  await success('login', { key: nsecEncode(secret) });
  try {
    const settings = (urls: string[]) =>
      success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: urls.join('\n') });
    await settings([a, b, gone]);
    const post = (id: string) => ({ author: 'alice', id, time: 1700000000, text: 'Post ' + id });
    await success('capture', { posts: [post('1'), post('2')] }, x);
    await until(() => relays.every(relay => relay.sent.length === 1));
    assert.equal((await success('status')).pending, 2);
    await settings([a, b]);
    const status = await success('status');
    assert.equal(status.pending, 2);
    assert.deepEqual(status.relays.map(relay => relay.pending), [2, 2]);
  } finally {
    held.resolve();
    await idle();
    await success('logout', { pubkey: owner });
  }
});

test('relay rows show live connections and acknowledgements while another relay is still broadcasting', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  await success('watch', { pubkey: owner });
  const slow = 'wss://progress-slow.example/',
    fast = 'wss://progress-fast.example/';
  const waiting = fixtureRelay(slow),
    healthy = fixtureRelay(fast);
  const gate = Promise.withResolvers<void>();
  waiting.holdPublish = gate.promise;
  t.after(() => gate.resolve());
  await success('settings', {
    pubkey: owner,
    handle: '',
    capture: false,
    relaySync: false,
    relays: slow + '\n' + fast,
  });
  await until(async () => !(await success('status')).running);
  const db = await openArchive('nostr-backup-' + owner);
  for (const created_at of [20, 21])
    await append(
      db,
      [
        {
          event: finalizeEvent(
            {
              kind: 1,
              created_at,
              tags: [],
              content: 'Progress fixture ' + created_at,
            },
            secret,
          ),
        },
      ],
      owner,
    );
  db.close();
  await success('broadcast');
  await until(
    async () =>
      waiting.sent.length === 1 &&
      (await success('status')).relays.find((relay) => relay.url === fast)?.phase ===
        'finished',
  );
  const active = await success('status');
  const held = active.relays.find((relay) => relay.url === slow)!;
  const complete = active.relays.find((relay) => relay.url === fast)!;
  assert.equal(active.running, true);
  assert.equal(held.connection, 'connected');
  assert.equal(held.phase, 'broadcasting');
  assert.equal(held.pending, 2);
  assert.equal(held.sent, 0);
  assert.equal(complete.connection, 'disconnected');
  assert.equal(complete.sent, 2);
  assert.equal(complete.pending, 0);
  assert.equal(healthy.sent.length, 2);
  const page = await archivePage(t);
  await until(() => pageElements(page)('reports').textContent.includes('Sending posts'));
  assert.equal(page.document.querySelectorAll('.relayRow').length, 2);
  assert.match(pageElements(page)('reports').textContent, /Connected/);
  assert.match(pageElements(page)('reports').textContent, /0 sent · 2 queued/);
  gate.resolve();
  await until(async () => !(await success('status')).running);
  const finished = await success('status');
  assert.ok(
    finished.relays.every(
      (relay) => relay.phase === 'finished' && relay.sent === 2 && relay.pending === 0,
    ),
  );
  assert.ok(finished.relays.every((relay) => relay.connection === 'disconnected'));
  assert.equal(finished.lastRun!.relays![slow].sent, 2);
  await success('logout', { pubkey: owner });
  assert.deepEqual((await success('status')).relays, []);
});

test('relay rows count age rejections separately from other failed deliveries and show the oldest signed date', async t => {
  const key = generateSecretKey(), owner = getPublicKey(key);
  const url = 'wss://age-limit.example/';
  const relay = fixtureRelay(url);
  relay.rejectBefore = Date.UTC(2020, 0) / 1000;
  await success('watch', { pubkey: owner });
  await success('settings', { pubkey: owner, handle: '', capture: false, relaySync: false, relays: url });
  await idle();
  const db = await openArchive('nostr-backup-' + owner);
  const events = [2013, 2014, 2023, 2024].map(year => finalizeEvent({
    kind: 1, created_at: Date.UTC(year, 0) / 1000, tags: [], content: 'Archived event ' + year,
  }, key));
  relay.reject = events[2].id;
  await append(db, events.map(event => ({ event })), owner);
  db.close();
  await success('broadcast');
  await idle();
  const row = (await success('status')).relays[0];
  assert.equal(row.pending, 3);
  assert.equal(row.sent, 1);
  assert.deepEqual(row.ageRejections, { count: 2, oldest: events[0].created_at });
  const page = await archivePage(t);
  await until(() => pageElements(page)('reports').textContent.includes('2 events for age this sync'));
  const text = pageElements(page)('reports').textContent;
  assert.match(text, /oldest was dated .*2013/);
  assert.match(text, /1 sent · 3 queued in total/);
  assert.doesNotMatch(text, /Asked to wait/);
  await success('logout', { pubkey: owner });
});

test('legacy single-account data reopens under its original identity after logout', async () => {
  const legacyKey = generateSecretKey(),
    legacyPub = getPublicKey(legacyKey);
  const conf = {
    pubkey: legacyPub,
    handle: '',
    capture: false,
    relaySync: false,
    relays: [],
  };
  const db = await openArchive('nostr-backup-v1');
  await append(
    db,
    [
      {
        event: finalizeEvent(
          { kind: 1, content: 'old archive', tags: [], created_at: 123 },
          legacyKey,
        ),
      },
    ],
    legacyPub,
  );
  db.close();
  const { writeAccountRecords } = await import('../src/account-storage.ts');
  await writeAccountRecords({ settings: conf });
  await success('logout', { pubkey: legacyPub });
  await success('login', { key: nsecEncode(legacyKey) });
  assert.equal((await success('status')).conf.archiveName, 'nostr-backup-v1');
  assert.deepEqual((await success('status')).conf.relays, DEFAULT_RELAYS);
  assert.equal((await success('status')).conf.relaySync, true);
  assert.equal((await success('status')).events, 1);
  await success('logout', { pubkey: legacyPub });
});
after(async () => {
  const db = await openArchive('nostr-backup-' + pubkey);
  db.close();
});

test('X capture links same-day matches locally, preserves original text and honors opt-out', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  const url = 'wss://duplicate-check.example/';
  const relay = fixtureRelay(url);
  await success('login', { key: nsecEncode(secret) });
  const conf = { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: url };
  await success('settings', conf);
  const db = await openArchive('nostr-backup-' + owner);
  const time = 1700000000;
  const first =
    'Today we released a small update so everyone can download their photos and keep a local copy.';
  const second =
    'The desktop version also supports saving all of your files together with their original names.';
  const event = finalizeEvent(
    { kind: 1, created_at: time, tags: [], content: first + '\n\n' + second },
    secret,
  );
  await append(db, [{ event }], owner);
  const post = (id: string, text: string, date = time) => ({
    author: 'alice',
    id,
    time: date,
    text,
  });
  const exact = post('1', first);
  const near = post(
    '2',
    second.replace('desktop version', 'desktop app').replace('also supports', 'now supports'),
  );
  const distinct = post('3', first.replace('everyone can', 'everyone cannot'));
  const anotherDay = post('4', first, time + 86400);
  const posts = new Map([exact, near, distinct, anotherDay, post('5', first)].map(p => [p.id, p]));
  t.mock.method(globalThis, 'fetch', (url: URL) => {
    const original = posts.get(String(url).split('/').pop()!)!;
    return publicTweet(url, original.text, original.time);
  });
  try {
    await success('capture', { posts: [exact, near, distinct, anotherDay] }, x);
    await idle();
    assert.deepEqual((await success('status')).x, {
      captured: 4,
      matched: 2,
      signed: 2,
      drafts: 0,
    });
    assert.deepEqual(
      relay.sent.map((e) => e.content).sort(),
      [distinct.text, anotherDay.text].sort(),
    );
    for (const source of [exact, near]) {
      assert.equal(await db.get('sources', 'x:' + source.id), event.id);
      assert.deepEqual(await db.get('xMatches', 'x:' + source.id), { ...source, visibility: 'public' });
    }
    const page = await archivePage(t);
    const field = pageElements(page)('skipCrossposts');
    await until(() => field.checked && pageElements(page)('xMatched').textContent === '2');
    // The switch saves as soon as it changes.
    field.click();
    await until(async () => (await success('status')).conf.skipCrossposts === false);
    await success('capture', { posts: [exact, near, post('5', first)] }, x);
    await idle();
    assert.deepEqual((await success('status')).x, {
      captured: 5,
      matched: 2,
      signed: 3,
      drafts: 0,
    });
    assert.equal(relay.sent.length, 3);
  } finally {
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('daily polling stays disconnected between checks and reports only successful syncs', async (t) => {
  t.mock.method(globalThis, 'fetch', (url: URL) => publicTweet(url, 'A new capture'));
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  const url = 'wss://daily.example/',
    relay = fixtureRelay(url);
  const epoch = Date.now();
  let now = epoch;
  t.mock.method(Date, 'now', () => now);
  const conf = { pubkey: owner, handle: 'alice', capture: true, relaySync: true, relays: url };
  await success('login', { key: nsecEncode(secret) });
  await success('settings', conf);
  await idle();
  const wake = async () => {
    api.alarms.onAlarm.list[0]({ name: 'backup' });
    await idle();
  };
  const status = async () => (await success('status')).relays.find((r) => r.url === url)!;
  try {
    const count = relay.filters.length;
    assert.equal(count, 1);
    assert.equal((await status()).lastSyncedAt, epoch);
    now += 3600000;
    await success(
      'capture',
      { posts: [{ author: 'alice', id: '9', time: 1700000000, text: 'A new capture' }] },
      x,
    );
    await idle();
    assert.equal(relay.sent.length, 1);
    assert.equal((await status()).lastSyncedAt, epoch);
    const opened = wireOpens.filter((address) => address === url).length;
    await wake();
    assert.equal(wireOpens.filter((address) => address === url).length, opened);
    assert.equal(relay.filters.length, count);
    now = epoch + 86400000;
    await wake();
    assert.equal(relay.filters.length, count + 1);
    assert.equal((await status()).lastSyncedAt, now);
    const page = await archivePage(t);
    await until(() =>
      pageElements(page)('lastSynced').textContent.includes(syncedAt(now)),
    );
    await success('settings', { ...conf, relaySync: false });
    await idle();
    now += 86400000;
    await wake();
    assert.equal(relay.filters.length, count + 1);
    relay.negLimit = ['rate-limited: wait', '5000'];
    await success('sync');
    await idle();
    assert.equal((await status()).lastSyncedAt, epoch + 86400000);
    assert.equal((await status()).phase, 'retry');
    now += 5000;
    relay.negLimit = undefined;
    await success('sync');
    await idle();
    assert.equal((await status()).lastSyncedAt, now);
  } finally {
    await success('logout', { pubkey: owner });
  }
});

test('X images are archived locally, retried on the selected Blossom server and signed with its CDN URL', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  const relayURL = 'wss://x-image.example/',
    relay = fixtureRelay(relayURL);
  const source = 'https://pbs.twimg.com/media/photo?format=png&name=large';
  const image = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 1]);
  const digest = Buffer.from(await crypto.subtle.digest('SHA-256', image)).toString('hex');
  const custom = 'https://media.example',
    cdnURL = 'https://cdn.example/' + digest + '.png';
  const gate = Promise.withResolvers<void>(), localGate = Promise.withResolvers<void>();
  const local = 'http://127.0.0.1:8081';
  let downloads = 0,
    uploads = 0;
  t.mock.method(globalThis, 'fetch', async (url: RequestInfo | URL, init: RequestInit = {}) => {
    if (String(url) === 'https://x.com/alice/status/192') return publicTweet(url, 'Plain text');
    if (/^https:\/\/x\.com\/alice\/status\/(191|193)$/.test(String(url))) {
      const postId = String(url).split('/').pop()!;
      const data = { data: { tweet_result_by_rest_id: { result: {
        __typename: 'Tweet', rest_id: postId, core: { user_results: { result: { core: { screen_name: 'alice' } } } },
        details: { full_text: postId === '191' ? 'Photo' : '', created_at_ms: 1700000000000 },
        media_entities2: [{ type: 'photo', expanded_url: url + '/photo/1', media_url_https: source }],
      } } } };
      const html = '<meta property="og:url" content="' + url + '"><meta property="og:description" content="' +
        (postId === '191' ? 'Photo' : '') + '">' +
        '<script>const data=' + JSON.stringify(JSON.stringify(data)) + '</script>';
      const page = new Response(html, { headers: { 'Content-Type': 'text/html' } });
      Object.defineProperty(page, 'url', { value: String(url) });
      return page;
    }
    if (String(url) === source) {
      downloads++;
      return new Response(image, { headers: { 'Content-Type': 'image/png' } });
    }
    uploads++;
    if (String(url) === 'https://blossom.band/upload') {
      await gate.promise;
      return new Response('', { status: 503 });
    }
    if (String(url) === local + '/upload') {
      await localGate.promise;
      return Response.json({url:local + '/' + digest + '.png',sha256:digest,size:image.length});
    }
    assert.equal(String(url), custom + '/upload');
    assert.equal(init.method, 'PUT');
    assert.equal(init.credentials, 'omit');
    const headers = new Headers(init.headers);
    const auth = JSON.parse(
      Buffer.from(headers.get('Authorization')!.slice(6), 'base64').toString(),
    );
    assert.ok(verifyEvent(auth));
    assert.equal(auth.pubkey, owner);
    assert.ok(auth.tags.some((t) => t[0] === 'x' && t[1] === digest));
    assert.ok(auth.tags.some((t) => t[0] === 'server' && t[1] === 'media.example'));
    return Response.json({ url: cdnURL, sha256: digest, size: image.length, type: 'image/png' });
  });
  await success('login', { key: nsecEncode(secret) });
  const db = await openArchive('nostr-backup-' + owner);
  try {
    const conf = {
      pubkey: owner,
      handle: 'alice',
      capture: true,
      relaySync: false,
      relays: relayURL,
    };
    await success('settings', conf);
    const page = await archivePage(t);
    const fields = pageElements(page);
    await until(() => fields('blossomServer').value === 'https://blossom.band');
    const post = { id: '191', author: 'alice', time: 1700000000, text: 'Photo\n\n' + source };
    await success('capture', { posts: [post] }, x);
    await until(() => uploads === 1);
    await success('capture', { posts: [{ ...post, id: '192', text: 'Plain text' }] }, x);
    await until(() => relay.sent.some((e) => e.content === 'Plain text'));
    assert.equal((await success('status')).x.media, 1);
    assert.equal(await db.get('sources', 'x:191'), undefined);
    assert.equal(await db.count('mediaFiles'), 1);
    gate.resolve();
    await until(async () => (await success('status')).x.mediaError?.includes('503'));
    fields('blossomServer').value = local + '\n' + custom;
    page.document.getElementById('blossomServer')!.dispatchEvent(new page.Event('input', { bubbles: true }));
    fields('backupSettings').requestSubmit();
    await until(() => relay.sent.some((e) => e.content.includes(cdnURL)));
    const id = await db.get('sources', 'x:191');
    const signed = await db.get('events', id!);
    assert.ok(signed && verifyEvent(signed));
    assert.equal(signed.content, 'Photo\n\n' + cdnURL);
    assert.equal(signed.tags.filter((t) => t[0] === 'client').length, 1);
    assert.ok(signed.tags.some((t) => t[0] === 'client' && t[1] === 'Social Backup'));
    assert.ok(signed.tags.some((t) => t[0] === 'imeta' && t.includes('x ' + digest)));
    assert.equal(JSON.stringify(signed).includes('twimg.com'), false);
    assert.equal((await db.get('mediaRefs', [id!, cdnURL]))?.status, 'saved');
    assert.deepEqual((await db.get('mediaFiles', digest))?.copies, [custom]);
    assert.ok(!JSON.stringify(signed).includes('127.0.0.1'));
    localGate.resolve();
    await until(async () => (await db.get('mediaFiles',digest))?.copies.includes(local));
    await success('capture', { posts: [post, { ...post, id: '193', text: source }] }, x);
    await until(() => relay.sent.length === 3);
    assert.equal(downloads, 1);
    assert.equal(uploads, 3);
    assert.equal((await success('status')).x.media, undefined);
  } finally {
    gate.resolve();
    localGate.resolve();
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('post backup is on by default for every platform and waits for a username', async () => {
  const target = 'wss://default-capture.example/';
  fixtureRelay(target);
  const save = (pubkey: string) => success('settings', { pubkey, handle: '', capture: true, relays: target, relaySync: false,
    social: { instagram: { handle: '', capture: true }, facebook: { handle: '', capture: true } } });
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  await success('login', { key: nsecEncode(secret) });
  try {
    const { conf } = await success('status');
    assert.equal(conf.capture, true);
    assert.equal(conf.social?.instagram?.capture, true);
    assert.equal(conf.social?.facebook?.capture, true);
    await save(owner);
    const post = { author: 'alice', id: '1', time: 1700000000, text: 'Post 1' };
    assert.equal((await call('capture', { posts: [post] }, x)).ok, false);
    assert.equal((await success('captureConfig', {}, x)).handle, '');
  } finally {
    await idle();
    await success('logout', { pubkey: owner });
  }
  // A read-only profile can save its sources with the default switches on.
  const viewer = getPublicKey(generateSecretKey());
  await success('watch', { pubkey: viewer });
  try {
    await save(viewer);
  } finally {
    await idle();
    await success('logout', { pubkey: viewer });
  }
});

function publicFacebookFixture(link: string, caption: string) {
  const path = new URL(link).pathname;
  return new Response('<div data-testid="newsFeedStream"><div role="feed"><a href="/alice"><img aria-label="Alice"></a>' +
    '<a href="' + path + '"><abbr data-utime="1600000000"></abbr></a><div data-testid="post_message">' +
    caption + '</div></div></div>', { headers: { 'content-type': 'text/html' } });
}

test('social profile backup only signs server-confirmed posts and keeps restricted captures local', async t => {
  const secret = generateSecretKey(), owner = getPublicKey(secret);
  const target = 'wss://profile-backup.example/';
  const relay = fixtureRelay(target);
  const origin = 'https://www.instagram.com';
  const ig = { ...x, url: origin + '/alice/', tab: { id: 17, url: origin + '/alice/' } };
  const fb = { ...x, url: 'https://www.facebook.com/alice', tab: { id: 18, url: 'https://www.facebook.com/alice' } };
  const base = { source: 'instagram', author: 'alice', time: 1700000000, text: 'Caption', visibility: 'public' };
  const instagram = (id: string, visibility: string) =>
    ({ ...base, id, url: origin + '/p/' + id + '/', visibility });
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    const url = String(input), link = origin + '/p/Public123/';
    if (url.startsWith('https://www.facebook.com/plugins/post.php?'))
      return publicFacebookFixture(fb.url + '/posts/123456', 'Verified Facebook caption');
    const body = url === link
      ? '<link rel="canonical" href="' + link + '"><meta property="og:url" content="' + link +
        '"><meta property="og:type" content="article"><meta property="article:published_time" content="2020-09-13T12:26:42Z">'
      : '<div class="Embed" data-permalink="' + link + '" data-media-type="GraphImage"><div class="Header">' +
        '<a class="Username" href="' + origin + '/alice/"></a></div><div class="Content"><a class="EmbeddedMedia" href="' +
        link + '"><img class="EmbeddedMediaImage" src="https://scontent.cdninstagram.com/a.jpg"></a></div></div>';
    // Instagram returns full documents, including a doctype and head, to the background verifier.
    const response = new Response('<!doctype html><html><head><title>Instagram</title></head><body>' + body + '</body></html>',
      { headers: { 'content-type': 'text/html' } });
    Object.defineProperty(response, 'url', {value: url});
    return response;
  });
  await success('login', { key: nsecEncode(secret) });
  try {
    await success('settings', { pubkey: owner, handle: '', capture: false, relays: target, relaySync: false,
      social: { instagram: { handle: '@Alice', capture: true }, facebook: { handle: 'Alice', capture: true } } });
    assert.equal((await success('captureConfig', {}, ig)).handle, 'alice');
    const posts = [instagram('Public123', 'public'), instagram('Private123', 'private'), instagram('Unknown123', 'unknown')];
    await success('capture', { posts }, ig);
    await success('capture', { posts }, ig);
    const other = { ...base, source: 'facebook', id: '123456', url: fb.url + '/posts/123456', text: 'Facebook post' };
    assert.equal((await call('capture', { posts: [other] }, ig)).ok, false);
    assert.equal((await call('capture', { posts }, { ...ig, tab: { id: 17, url: origin + '/other/' } })).ok, false);
    assert.equal((await call('capture', { posts }, { ...ig, url: 'https://instagram.com/alice/' })).ok, false);
    await success('capture', { posts: [other] }, fb);
    await until(() => relay.sent.length === 2);
    // All three posts are in the backup and get a mark. Only the public one has a copy on Nostr to open.
    assert.deepEqual(await success('savedPosts', { ids: ['Public123', 'Private123', 'Unknown123', 'Missing123'] }, ig),
      [{ id: 'Public123', onNostr: true }, { id: 'Private123', onNostr: false }, { id: 'Unknown123', onNostr: false }]);
    tabURLs.length = 0;
    assert.equal(await success('openSaved', { id: 'Private123' }, ig), false);
    assert.equal(await success('openSaved', { id: 'Public123' }, ig), true);
    const { conf, social } = await success('status');
    assert.deepEqual(social, { instagram: { saved: 3, signed: 1 }, facebook: { saved: 1, signed: 1 } });
    const db = await openArchive(conf.archiveName);
    try {
      const { dump, queueBroadcast } = await import('../src/archive.ts');
      const { unzipSync, strFromU8 } = await import('fflate');
      const files = unzipSync(await dump(db, owner));
      assert.equal(JSON.parse(strFromU8(files['profile-captures.json'])).length, 4);
      assert.equal(Object.keys(JSON.parse(strFromU8(files['social-sources.json']))).length, 2);
      assert.equal(await queueBroadcast(db, conf), 2);
      const events = await db.getAll('events');
      assert.equal(events.length, 2);
      assert.ok(events.some(event => event.content === 'Verified Facebook caption' && event.created_at === 1600000000));
      assert.ok(events.every(event => event.content !== 'Facebook post' && !event.content.includes('Caption')));
      assert.ok(events.every(event => verifyEvent(event)));
      assert.deepEqual(new Set(events.map(event => event.tags.find(tag => tag[0] === 'r')?.[1])),
        new Set([posts[0].url, other.url]));
      assert.equal(await db.count('drafts'), 0);
      assert.deepEqual(tabURLs, ['https://iris.to/' + noteEncode((await db.get('sources', 'instagram:Public123'))!)]);
      await success('capture', { posts: [instagram('Public123', 'private')] }, ig);
      assert.equal((await db.get('localCaptures', 'instagram:Public123'))?.visibility, 'private');
      assert.equal(await queueBroadcast(db, conf), 1);
      assert.equal(await db.count('heldEvents'), 1);
      // A post that turned private keeps its mark, and its copy is no longer offered.
      assert.deepEqual(await success('savedPosts', { ids: ['Public123'] }, ig), [{ id: 'Public123', onNostr: false }]);
      assert.equal(await success('openSaved', { id: 'Public123' }, ig), false);
      assert.equal(tabURLs.length, 1);
    } finally { db.close(); }
  } finally { await success('logout', { pubkey: owner }); }
});

test('self-quotes fetch and archive public originals on all three networks before signing, without relay receipts', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret),
    relayURL = 'wss://quote.example/';
  const relay = fixtureRelay(relayURL),
    held = Promise.withResolvers<void>();
  relay.holdPublish = held.promise;
  const xLink = 'https://x.com/alice/status/10',
    fb = 'https://www.facebook.com/alice/posts/22',
    ig = 'https://www.instagram.com/p/MyPost/',
    other = 'https://www.instagram.com/p/Theirs/';
  const text = 'Read these: ' + [xLink, fb, ig, other, 'https://x.com/bob/status/11'].join(' ');
  const html = (url: string, body: string) => {
    const response = new Response(body, { headers: { 'content-type': 'text/html' } });
    Object.defineProperty(response, 'url', { value: url });
    return response;
  };
  const reads: string[] = [];
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL, opts?: RequestInit) => {
    const url = String(input);
    assert.equal(opts?.credentials, 'omit');
    if (url === 'https://x.com/alice/status/99' || url === 'https://x.com/alice/status/98') return publicTweet(input, text);
    reads.push(url);
    if (url === xLink) {
      const data = {
        data: {
          tweet_result_by_rest_id: {
            result: {
              __typename: 'Tweet',
              rest_id: '10',
              core: {
                user_results: {
                  result: { core: { screen_name: 'alice' }, privacy: { protected: false } },
                },
              },
              details: { full_text: 'First on X', created_at_ms: 1600000000000 },
              media_entities2: [],
            },
          },
        },
      };
      return html(url, '<script>const data=' + JSON.stringify(JSON.stringify(data)) + '</script>');
    }
    if (url.startsWith('https://www.facebook.com/plugins/post.php?')) {
      assert.equal(new URL(url).searchParams.get('href'), fb);
      return html(
        url,
        '<div data-testid="newsFeedStream"><div role="feed"><a href="/alice"><img aria-label="Alice"></a>' +
          '<a href="/alice/posts/22"><abbr data-utime="1600000001"></abbr></a><div data-testid="post_message">First on Facebook</div></div></div>',
      );
    }
    for (const [link, author] of [
      [ig, 'alice'],
      [other, 'bob'],
    ]) {
      if (url === link + 'embed/captioned/')
        return html(
          url,
          '<div class="Embed" data-permalink="' +
            link +
            '" data-media-type="GraphImage">' +
            '<div class="Header"><a class="Username" href="https://www.instagram.com/' +
            author +
            '/"></a></div><div class="Caption"><a class="CaptionUsername" href="https://www.instagram.com/' +
            author +
            '/">Byline</a>First on Instagram</div>' +
            '<div class="Content"><a class="EmbeddedMedia" href="' +
            link +
            '"><img class="EmbeddedMediaImage" src="https://scontent.cdninstagram.com/a.jpg"></a></div></div>',
        );
    }
    if (url === ig)
      return html(
        url,
        '<link rel="canonical" href="' +
          ig +
          '"><meta property="og:type" content="article"><meta property="og:url" content="' +
          ig +
          '"><meta property="article:published_time" content="2020-09-13T12:26:42Z">',
      );
    throw Error('Unexpected lookup ' + url);
  });
  await success('login', { key: nsecEncode(secret) });
  await success('settings', {
    pubkey: owner,
    handle: 'alice',
    relays: relayURL,
    capture: true,
    relaySync: false,
    social: {
      instagram: { handle: 'alice', capture: true },
      facebook: { handle: 'alice', capture: true },
    },
  });
  const db = await openArchive('nostr-backup-' + owner);
  try {
    const post = { id: '99', author: 'alice', text, time: 1700000000 };
    await success('capture', { posts: [post] }, x);
    await until(async () => !!(await db.get('sources', 'x:99')));
    const find = async (key: string) => (await db.get('events', (await db.get('sources', key))!))!;
    const parent = await find('x:99');
    assert.ok(verifyEvent(parent));
    const references = [...parent.content.matchAll(/nostr:(nevent1[a-z0-9]+)/g)].map((m) =>
      decode(m[1]),
    );
    for (const [i, key] of ['x:10', 'facebook:22', 'instagram:MyPost'].entries()) {
      const original = await find(key);
      assert.ok(verifyEvent(original));
      assert.deepEqual(parent.tags.filter((tag) => tag[0] === 'q')[i], [
        'q',
        original.id,
        '',
        owner,
      ]);
      const ref = references[i];
      assert.ok(ref.type === 'nevent' && ref.data.id === original.id && ref.data.author === owner);
    }
    for (const url of [xLink, fb, ig]) assert.ok(!parent.content.includes(url));
    assert.ok(
      parent.content.includes(other) && parent.content.includes('https://x.com/bob/status/11'),
    );
    assert.equal(await db.count('outbox'), 4);
    const before = reads.length;
    await success('capture', { posts: [{ ...post, id: '98' }] }, x);
    await until(async () => !!(await db.getKey('sources', 'x:98')));
    assert.equal(reads.length, before);
    assert.equal(await db.count('events'), 5);
  } finally {
    held.resolve();
    await idle();
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('unavailable own quote stays a local draft, and resolves when the original is later captured', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret);
  const site = 'https://www.facebook.com/alice',
    original = site + '/posts/100';
  const sender = { ...x, url: site, tab: { id: 17, url: site } };
  const relayURL = 'wss://quote-later.example/',
    relay = fixtureRelay(relayURL);
  let released = false;
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    const link = new URL(String(input)).searchParams.get('href');
    if (link === site + '/posts/101') return publicFacebookFixture(link, 'A quote: ' + original);
    if (link === site + '/posts/102') return publicFacebookFixture(link, 'Unrelated');
    if (released && link === original) return publicFacebookFixture(link, 'Released original');
    return new Response('', {status: 503});
  });
  await success('login', { key: nsecEncode(secret) });
  await success('settings', {
    pubkey: owner,
    handle: '',
    capture: false,
    relays: relayURL,
    relaySync: false,
    social: { facebook: { handle: 'alice', capture: true } },
  });
  const db = await openArchive('nostr-backup-' + owner);
  const post = (id: string, text: string, visibility: string) => ({
    source: 'facebook',
    author: 'alice',
    time: 1600000000,
    id,
    text,
    url: site + '/posts/' + id,
    visibility,
  });
  try {
    await success(
      'capture',
      {
        posts: [
          post('101', 'A quote: ' + original, 'public'),
          post('100', 'Restricted', 'private'),
          post('102', 'Unrelated', 'public'),
        ],
      },
      sender,
    );
    await idle();
    assert.ok(await db.getKey('drafts', 'facebook:101'));
    assert.ok(await db.getKey('localCaptures', 'facebook:100'));
    assert.ok(!(await db.getKey('sources', 'facebook:100')));
    assert.deepEqual(
      relay.sent.map((e) => e.content),
      ['Unrelated'],
    );
    released = true;
    await success('capture', { posts: [post('100', 'Released original', 'public')] }, sender);
    await idle();
    const id = await db.get('sources', 'facebook:100');
    const parent = await db.get('events', (await db.get('sources', 'facebook:101'))!);
    assert.deepEqual(
      parent?.tags.find((t) => t[0] === 'q'),
      ['q', id, '', owner],
    );
    assert.equal(await db.count('drafts'), 0);
  } finally {
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('X self-replies become Nostr replies to their parents, fetching missing earlier posts of the thread first', async (t) => {
  const secret = generateSecretKey(),
    owner = getPublicKey(secret),
    relayURL = 'wss://self-replies.example/';
  fixtureRelay(relayURL);
  const time = 1700000000;
  const crossposted = 'We moved the whole archive to a new server today, and every old link still works as before.';
  const posts: Record<string, [text: string, parent?: string]> = {
    100: ['Thread start'], 101: ['Second', 'alice/100'], 102: ['Third', 'alice/101'],
    200: ['For Bob', 'bob/50'], 201: ['More for Bob', 'alice/200'],
    300: ['Parent gone', 'alice/299'], 400: [crossposted, 'alice/100'],
    500: ['What I told Bob: https://x.com/alice/status/200'],
  };
  const reads: string[] = [];
  t.mock.method(globalThis, 'fetch', async (input: RequestInfo | URL) => {
    const url = String(input), [, author, , id] = new URL(url).pathname.split('/');
    reads.push(id);
    const [text, parent] = posts[id] ?? [];
    if (author !== 'alice' || !text) return new Response('', { status: 503 });
    const [parentAuthor, parentId] = parent?.split('/') ?? [];
    const response = new Response(publicTweetHTML('alice', id, text, time, [], parent ? {
      reply_to_results: { rest_id: parentId },
      reply_to_user_results: { result: { core: { screen_name: parentAuthor } } },
    } : {}), { headers: { 'content-type': 'text/html' } });
    Object.defineProperty(response, 'url', { value: url });
    return response;
  });
  await success('login', { key: nsecEncode(secret) });
  await success('settings', { pubkey: owner, handle: 'alice', capture: true, relaySync: false, relays: relayURL });
  const db = await openArchive('nostr-backup-' + owner);
  // The same text was already posted on Nostr, so that X reply is a crosspost, not a new note.
  const note = finalizeEvent({ kind: 1, created_at: time, tags: [], content: crossposted }, secret);
  await append(db, [{ event: note }], owner);
  const event = async (id: string) => (await db.get('events', (await db.get('sources', 'x:' + id))!))!;
  const named = (e: NostrEvent, ...names: string[]) => e.tags.filter(([name]) => names.includes(name));
  try {
    // Only the newest posts of each thread are on screen.
    const shown = ['102', '201', '300', '400', '500'].map((id) => ({ id, author: 'alice', time, text: posts[id][0] }));
    await success('capture', { posts: shown }, x);
    await until(async () => (await db.getKey('sources', 'x:102')) && (await db.getKey('sources', 'x:201')) &&
      (await db.getKey('sources', 'x:500')));
    const [head, second, third, comment, followUp] = await Promise.all(['100', '101', '102', '200', '201'].map(event));
    assert.deepEqual([head, second, third, comment, followUp].map((e) => e.kind), [1, 1, 1, 1111, 1111]);
    assert.deepEqual(named(head, 'e', 'p'), []);
    assert.deepEqual(named(second, 'e', 'p'), [['e', head.id, '', 'root', owner], ['p', owner]]);
    assert.deepEqual(named(third, 'e', 'p'),
      [['e', head.id, '', 'root', owner], ['e', second.id, '', 'reply', owner], ['p', owner]]);
    assert.deepEqual(named(followUp, 'I', 'e', 'k'),
      [['I', 'https://x.com/bob/status/50'], ['e', comment.id, '', owner], ['k', '1111']]);
    // No reply to your own post points at that post's X address.
    for (const [reply, parent] of [[second, '100'], [third, '101'], [followUp, '200']] as const)
      assert.ok(!reply.tags.some(([, value]) => value.endsWith('/status/' + parent)));
    assert.equal(await db.get('sources', 'x:400'), note.id);
    // Quoting your own comment on someone else's post links its signed copy.
    const quote = await event('500');
    assert.deepEqual(named(quote, 'q'), [['q', comment.id, '', owner]]);
    const [, quoted] = /nostr:(nevent1\w+)/.exec(quote.content)!;
    assert.deepEqual(decode(quoted).data, { id: comment.id, author: owner, kind: 1111, relays: [] });
    // Without its earlier post, a reply waits on this device instead of losing its thread.
    assert.ok(await db.getKey('drafts', 'x:300'));
    assert.ok(!(await db.getKey('sources', 'x:300')));
    assert.equal(reads.filter((id) => id === '100').length, 1);
  } finally {
    db.close();
    await success('logout', { pubkey: owner });
  }
});

test('profile edits merge concurrent fields, retain signed versions and recover only selected follows', async () => {
  const secret = generateSecretKey(), account = getPublicKey(secret);
  const url = 'wss://profile-editor.example/', relay = fixtureRelay(url);
  const tags = [['custom', 'opaque', 'preserved']];
  const profile = (created_at: number, fields: object) => finalizeEvent({kind: 0, created_at, content: JSON.stringify(fields), tags}, secret);
  const old = profile(10, {name: 'Alice', about: 'Original', custom: [false, 42]});
  const concurrent = profile(11, {name: 'Alice', about: 'Original', custom: [false, 42], website: 'https://example.com'});
  relay.stored.set(old.id, old);
  await success('login', {key: nsecEncode(secret)});
  try {
    await success('settings', { pubkey: account, handle: '', capture: false, relaySync: false, relays: url });
    assert.equal((await success('eventVersions', {account, kind: 0, refresh: true}))[0].id, old.id);
    relay.stored.set(concurrent.id, concurrent);
    const exact = '{"id":9007199254740995,"samples":[1.000000000000000000009,1e-900]}';
    const content = '{"name":"Alice","about":"Edited","custom":[false,42],"milestone":'+exact+'}';
    const edited = await success('editProfile', {account, baseId: old.id, content});
    assert.equal(verifyEvent(edited), true);
    assert.ok(edited.content.includes(exact));
    assert.deepEqual(edited.tags, tags);
    assert.deepEqual(JSON.parse(edited.content), {...JSON.parse(content), website: 'https://example.com'});
    assert.deepEqual((await success('eventVersions', {account, kind: 0})).map(e => e.id), [edited.id, concurrent.id, old.id]);
    assert.equal((await call('editProfile', {account, baseId: old.id, content: '{"about":"Another edit"}'})).ok, false);
    for (const body of [{content: '{}'}, {content: JSON.stringify({...JSON.parse(edited.content), test: {private: nsecEncode(secret)}})}])
      assert.equal((await call('editProfile', {account, baseId: edited.id, ...body})).ok, false);
    assert.equal((await call('eventVersions', {account, kind: 0}, x)).ok, false);

    const alex = getPublicKey(generateSecretKey()), sam = getPublicKey(generateSecretKey()), later = getPublicKey(generateSecretKey());
    const original = finalizeEvent({kind: 3, created_at: 10, tags: [['p', alex, '', 'Alex'], ['p', sam]], content: 'old'}, secret);
    relay.stored.set(original.id, original);
    await success('eventVersions', {account, kind: 3, refresh: true});
    const partial = finalizeEvent({kind: 3, created_at: 11, tags: [['p', later], ['custom', 'keep']], content: 'current'}, secret);
    relay.stored.set(partial.id, partial);
    await success('eventVersions', {account, kind: 3, refresh: true});
    const restored = await success('recoverFollows', {account, baseId: partial.id, fromId: original.id, pubkeys: [alex]});
    assert.deepEqual(restored.tags, [...partial.tags, ['p', alex, '', 'Alex']]);
    assert.equal(restored.content, partial.content);
    assert.equal(verifyEvent(restored), true);
  } finally { await idle(); await success('logout', {pubkey: account}); }
});
