# Changelog

## 2.0.10 - 2026-03-25

- Refined the Tasks tab selector hover and selection styling to remove the awkward square overlay artifact.
- Fixed the Inspector tab chips so the selected state no longer renders the broken default icon/check treatment.
- Bumped app, sidecar, and packaging metadata to 2.0.10.

## 2.0.9 - 2026-03-24

- Fixed the Android native bridge build issues and bumped app, sidecar, and packaging metadata to 2.0.9.

## 2.0.8 - 2026-03-22

- Improved Event Log and inspector trace rendering with grouped send/response cards.
- Added separate log text scaling and phone-oriented layout updates.
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

- Rebuilt the project as `S3 Browser Crossplat`, a Flutter-based cross-platform S3 browser targeting Windows, macOS, Linux, and Android.
- Added a desktop sidecar engine model with packaged Python, Go, Rust, and Java engines, with the Python engine implemented as the first real S3 backend.
- Added Windows MSI packaging and engine staging so desktop releases bundle the app plus the engine sidecars under `engines/`.
- Added a benchmark workspace, adaptive navigation shell, clustered settings, and an Event Log workspace with export support.
- Added endpoint profile management with secure-secret integration points, profile testing, selection, creation, and deletion.
- Added bucket and object browsing flows with real Python-backed bucket listing, object listing, object details, versions, tags, headers, presigned URLs, and bucket admin inspection.
- Added bucket creation with versioning and object-lock options.
- Added transfer, diagnostics, debug logging, and busy-state feedback improvements so actions immediately show progress in the UI.
- Added portable bootstrap and build scripts that stage local toolchains and produce self-contained Windows artifacts.

## 1.x

- Legacy Python Tkinter S3 browser implementation before the `S3 Browser Crossplat` rewrite.
