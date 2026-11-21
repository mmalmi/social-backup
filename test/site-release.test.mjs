import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { parseHTML } from 'linkedom';
import { createSiteReleasePlan, runSiteRelease } from '../website/release-site.mjs';

const gatePath = fileURLToPath(import.meta.url);

test('each install link downloads the matching browser archive with the package version in its name', async t => {
  const root = await mkdtemp(join(tmpdir(), 'social-backup-site-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  for (const dir of ['dist', 'ui', 'website/public']) await mkdir(join(root, dir), { recursive: true });
  await copyFile(new URL('../website/public/index.html', import.meta.url), join(root, 'website/public/index.html'));
  await writeFile(join(root, 'package.json'), JSON.stringify({ version: '1.20.3' }));
  await writeFile(join(root, 'ui/icon.svg'), '<svg/>');
  const browsers = ['chrome', 'firefox', 'safari'];
  for (const browser of browsers) await writeFile(join(root, `dist/social-backup-${browser}.zip`), browser);

  execFileSync(process.execPath, ['--experimental-strip-types', fileURLToPath(new URL('../scripts/build-site.ts', import.meta.url))], { cwd: root });
  const site = join(root, 'website/dist');
  const { document } = parseHTML(await readFile(join(site, 'index.html'), 'utf8'));
  const links = [...document.querySelectorAll('#install a[download]')];
  assert.equal(links.length, browsers.length);
  for (const browser of browsers) {
    const link = links.find(link => link.textContent.toLowerCase().includes(browser));
    const href = `./downloads/social-backup-${browser}-1.20.3.zip`;
    assert.equal(link?.getAttribute('href'), href);
    assert.equal(await readFile(join(site, href), 'utf8'), browser);
  }
});

test('both website release paths retain the wallet gate and website build before native verification', () => {
  for (const deploy of [false, true]) {
    const steps = createSiteReleasePlan({ deploy, gatePath });
    assert.deepEqual(steps.map(step => step.id), deploy ? ['test', 'build', 'publish', 'deploy'] : ['test', 'build', 'publish']);
    assert.deepEqual(steps[0].command, ['pnpm', 'test:release']);
    assert.deepEqual(steps[1].command, ['pnpm', 'run', 'build:site']);
    assert.deepEqual(steps[2].command, [
      process.execPath, gatePath, '--dist', steps[2].cwd,
      '--tree', 'nostr-backup-site', '--htree', process.env.HTREE_BIN || 'htree',
    ]);
  }
});

test('live website release requires the native runner while dry-run remains available', () => {
  assert.throws(() => createSiteReleasePlan({ gatePath: '' }), /IRIS_NATIVE_RELEASE_GATE/);
  assert.equal(createSiteReleasePlan({ dryRun: true, gatePath: '' })[2].command[1], '$IRIS_NATIVE_RELEASE_GATE');
});

test('Cloudflare deploy is blocked if native verification fails or only returns a candidate', async () => {
  const success = { status: 0, stdout: '', stderr: '' };
  for (const rejected of [
    { status: 1, stdout: '', stderr: '' },
    { status: 0, stdout: 'url: nhash1ace\npublished: npub1example/nostr-backup-site-candidate-abcdef123456', stderr: '' },
  ]) {
    const seen = [];
    await assert.rejects(runSiteRelease({ deploy: true, gatePath }, async step => {
      seen.push(step.id);
      return step.id === 'publish' ? rejected : success;
    }));
    assert.deepEqual(seen, ['test', 'build', 'publish']);
  }
});
