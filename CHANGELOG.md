# Changelog

## 2.0.8 - 2026-03-18

- Fixed macOS desktop sandbox entitlements so outbound S3 traffic and Downloads-folder exports no longer fail with `io error` and `Operation not permitted`.
- Improved Event Log export UX with a visible export path plus direct open-log and reveal-in-folder actions.
- Moved staged macOS sidecars into `Contents/Resources/engines` and clean stale sidecars before Xcode assemble so repeated macOS builds do not fail during codesign.
- Renamed the user-facing product title to `S3 Browser Cross Platform` across the app shell and platform packaging metadata.
- Bumped app, sidecar, and packaging metadata to 2.0.8.

## 2.0.7 - 2026-03-16

- Promoted Tasks into a dedicated top-level workspace and expanded task detail cards for transfers, tools, and browser actions.
- Added richer transfer telemetry plumbing so desktop sidecars can stream detailed upload/download progress into the Tasks workspace.
- Improved browser create flows with a named Create prefix dialog, refined selected-profile readability, and expanded Windows Android APK build support.
- Bumped app, sidecar, and packaging metadata to 2.0.7.

## 2.0.6 - 2026-03-14

- Routed benchmark runs through the selected S3 profile so all sidecar engines execute real benchmark traffic.
- Updated the benchmark throughput area chart to stack per-operation metrics for total request visibility.
- Bumped app, sidecar, and packaging metadata to 2.0.6.

## 2.0.5 - 2026-03-12

- Improved browser object pagination with local 1000-item pages plus a show-all mode.
- Refined object, settings, task, and benchmark preview UX, including richer chart controls and preview image export.
- Bumped app, sidecar, and packaging metadata to 2.0.5.

## 2.0.4 - 2026-03-11

- Bump app, sidecar, and packaging metadata to 2.0.4.

## 2.0.3 - 2026-03-11

- Bump app, sidecar, and packaging metadata to 2.0.3.

## 2.0.0 - 2026-03-08

- Rebuilt the project as `S3 Browser Cross Platform`, a Flutter-based cross-platform S3 browser targeting Windows, macOS, Linux, and Android.
- Added a desktop sidecar engine model with packaged Python, Go, Rust, and Java engines, with the Python engine implemented as the first real S3 backend.
- Added Windows MSI packaging and engine staging so desktop releases bundle the app plus the engine sidecars under `engines/`.
- Added a benchmark workspace, adaptive navigation shell, clustered settings, and an Event Log workspace with export support.
- Added endpoint profile management with secure-secret integration points, profile testing, selection, creation, and deletion.
- Added bucket and object browsing flows with real Python-backed bucket listing, object listing, object details, versions, tags, headers, presigned URLs, and bucket admin inspection.
- Added bucket creation with versioning and object-lock options.
- Added transfer, diagnostics, debug logging, and busy-state feedback improvements so actions immediately show progress in the UI.
- Added portable bootstrap and build scripts that stage local toolchains and produce self-contained Windows artifacts.

## 1.x

- Legacy Python Tkinter S3 browser implementation before the `S3 Browser Cross Platform` rewrite.
