import 'fake-indexeddb/auto';
import test from 'node:test';
import assert from 'node:assert/strict';
import { openDB } from 'idb';
import { HashTree, sha256, toHex, fromHex, type Store } from '@hashtree/core';
import { DexieStore } from '@hashtree/dexie';
import { openMediaBlocks, readMediaFile } from '../src/media-files.ts';

test('legacy media blocks survive the storage upgrade, eviction and offline reopen', async () => {
  const name = crypto.randomUUID();
  const blocks = new Map<string, Uint8Array>();
  const store: Store = {
    get: async hash => blocks.get(toHex(hash)) ?? null,
    has: async hash => blocks.has(toHex(hash)),
    delete: async hash => blocks.delete(toHex(hash)),
    put: async (hash, bytes) => { blocks.set(toHex(hash), bytes); return true; },
  };
  const bytes = Uint8Array.from({ length: 2 * 1024 * 1024 + 23 }, (_, i) => i % 251);
  const { cid, size } = await new HashTree({ store }).putFile(bytes);
  const mediaHashes = new Set(blocks.keys());
  const mediaBytes = [...blocks.values()].reduce((total, block) => total + block.length, 0);
  const file = {
    sha256: toHex(await sha256(bytes)), size, mime: 'image/jpeg',
    cid: { hash: toHex(cid.hash), key: toHex(cid.key!) }, blocks: [...mediaHashes], copies: [],
  };
  // Cross the migration's page boundary, with evictable unrelated cache blocks.
  for (let i = 0; i < 70; i++) {
    const block = new Uint8Array([i]);
    blocks.set(toHex(await sha256(block)), block);
  }
  // Dexie v3 uses IndexedDB version 30. Seed the actual previous on-disk shape.
  const legacy = await openDB(name + '-media', 30, {
    upgrade(db) {
      for (const table of ['blobs', 'accesses'])
        db.createObjectStore(table, { keyPath: 'hashHex' }).createIndex('lastAccess', 'lastAccess');
    },
  });
  const tx = legacy.transaction(['blobs', 'accesses'], 'readwrite');
  for (const [hashHex, data] of blocks) {
    await tx.objectStore('blobs').put({ hashHex, data, lastAccess: 1 });
    await tx.objectStore('accesses').put({ hashHex, lastAccess: 7 });
  }
  await tx.done;
  legacy.close();
  let upgraded = openMediaBlocks({ name });
  try {
    assert.equal(await upgraded.totalBytes(), mediaBytes + 70);
    const inspect = await openDB(name + '-media');
    try {
      assert.equal((await inspect.get('accesses', file.blocks[0])).lastAccess, 7);
    } finally { inspect.close(); }
    assert.deepEqual(await readMediaFile(upgraded, file), bytes);
    assert.equal(await upgraded.evict(mediaBytes, mediaHashes), 70);
    assert.equal(await upgraded.totalBytes(), mediaBytes);
    upgraded.close();
    upgraded = openMediaBlocks({ name });
    assert.deepEqual(await readMediaFile(upgraded, file), bytes);
    for (const hash of mediaHashes) assert.equal(await upgraded.delete(fromHex(hash)), true);
    assert.equal(await upgraded.totalBytes(), 0);
  } finally {
    upgraded.close();
    await DexieStore.deleteDatabase(name + '-media');
  }
});
