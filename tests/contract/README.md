# Contract Test Suite

The contract suite is the enforcement point for engine parity.

Minimum scenarios:

1. `health` returns engine metadata.
2. `testProfile` maps invalid credentials to `auth_failed`.
3. `listBuckets` returns normalized bucket summaries.
4. `listObjects` honors `prefix`, `delimiter`, and cursor behavior.
5. `listObjectVersions` returns version and delete marker flags.
6. `startUpload` and `startDownload` return `TransferJob`.
7. `deleteObjects` returns partial failures without crashing.
8. `startBenchmark` produces a stable status schema and export path.

The fixtures in `tests/fixtures` are canonical shape examples for engine implementers.

