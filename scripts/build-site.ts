import { copyFile, cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';

const output = 'website/dist';
const { version } = JSON.parse(await readFile('package.json', 'utf8'));
await rm(output, { recursive: true, force: true });
await mkdir(output + '/downloads', { recursive: true });
await cp('website/public', output, { recursive: true });
await copyFile('ui/icon.svg', output + '/icon.svg');
let index = await readFile(output + '/index.html', 'utf8');
const redirects = [];
for (const browser of ['chrome', 'firefox', 'safari']) {
  const base = `social-backup-${browser}`;
  const archive = `${base}-${version}.zip`;
  await copyFile(`dist/${base}.zip`, `${output}/downloads/${archive}`);
  index = index.replaceAll(`{{${browser}Archive}}`, archive);
  redirects.push(`/downloads/${base}.zip /downloads/${archive} 302`);
}
await writeFile(output + '/index.html', index);
await writeFile(output + '/_redirects', redirects.join('\n') + '\n');
console.log('Built ' + output);
