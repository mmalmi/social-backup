"""Build both store packages from the exact source files supplied to Firefox reviewers."""
from hashlib import sha256
import json
from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

root = Path(__file__).resolve().parents[1]
version = json.loads((root / 'package.json').read_text())['version']
output = root / 'build' / f'store-{version}'
output.parent.mkdir(exist_ok=True)

inputs = ['package.json', 'pnpm-lock.yaml', 'tsconfig.json', 'LICENSE', 'src', 'test',
          'ui', 'scripts/build.ts', 'scripts/svelte.ts', 'scripts/test-release.ts', 'scripts/legacy-x-proofs.ts',
          'scripts/build-site.ts', 'website/public/index.html', 'website/release-site.mjs']
paths = run(['git', 'ls-files', '-z', '--', *inputs], cwd=root, check=True,
            capture_output=True).stdout.decode().split('\0')
sources = {name: root / name for name in paths if name}
sources['README.md'] = root / 'store/reviewer.md'

with TemporaryDirectory(dir=output.parent, prefix='store-build-') as temp:
    stage = Path(temp)
    source = stage / 'source'
    files = stage / 'packet'
    files.mkdir()
    with ZipFile(files / 'firefox-source.zip', 'w') as archive:
        for name, path in sorted(sources.items()):
            info = ZipInfo(name)
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes(), compress_type=ZIP_DEFLATED)
    with ZipFile(files / 'firefox-source.zip') as archive:
        archive.extractall(source)

    run(['pnpm', 'install', '--frozen-lockfile', '--ignore-workspace'], cwd=source, check=True)
    run(['pnpm', 'build'], cwd=source, check=True)
    for browser in ['chrome', 'firefox']:
        name = f'social-backup-{browser}.zip'
        (source / 'dist' / name).replace(files / name)
    with ZipFile(files / 'social-backup-firefox.zip') as archive:
        settings = json.loads(archive.read('manifest.json'))['browser_specific_settings']
        if 'gecko_android' in settings:
            raise ValueError('Firefox store submissions are desktop only. Remove the Android compatibility declaration.')

    for name in ['store/listing.md', 'store/reviewer.md', 'store/archive.png', 'store/promo.png',
                 'ui/icon-128.png', 'website/public/privacy.html']:
        (files / Path(name).name).write_bytes((root / name).read_bytes())
    sums = [f'{sha256(p.read_bytes()).hexdigest()}  {p.name}\n' for p in sorted(files.iterdir())]
    (files / 'SHA256SUMS').write_text(''.join(sums))
    output.mkdir(exist_ok=True)
    for path in sorted(files.iterdir(), key=lambda p: p.name == 'SHA256SUMS'):
        path.replace(output / path.name)
print(f'Store submissions: build/{output.name}/')
