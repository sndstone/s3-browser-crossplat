# macOS Packaging

`./scripts/build.sh macos` now produces:

- A release `.app` bundle from `flutter build macos`
- A distributable `.dmg` in `dist/macos/`

The DMG is assembled from the release bundle and staged desktop engines using `hdiutil`.

Codesigning and notarization are still not automated in this repo. The generated DMG is intended for local install and distribution workflows before signing identities are configured.
