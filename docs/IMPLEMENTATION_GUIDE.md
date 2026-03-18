# Implementation Guide

## Transport Model

Desktop engines run as sidecar processes. The Flutter shell talks to them through a versioned JSON transport. Android engines expose the same logical methods through platform channels and native adapters.

Desktop request envelope:

```json
{
  "requestId": "req-123",
  "method": "listObjects",
  "params": {},
  "engineVersion": "1.0"
}
```

Desktop response envelope:

```json
{
  "requestId": "req-123",
  "ok": true,
  "result": {},
  "error": null
}
```

Progress event envelope:

```json
{
  "event": "transferProgress",
  "payload": {
    "jobId": "transfer-1",
    "progress": 0.42
  }
}
```

## Pagination Rules

- `listBuckets` returns all visible buckets for the endpoint.
- `listObjects` must honor `prefix`, `delimiter`, and `cursor`.
- `cursor` is opaque to the UI and owned by the engine.
- Hierarchical mode uses `delimiter="/"`.
- Flat mode leaves `delimiter` empty.

## Transfer Rules

- Transfers are always represented as `TransferJob`.
- Start methods return immediately with a job descriptor.
- Progress is reported asynchronously.
- Cancel requests must be idempotent.
- Pause/resume may return `unsupported_feature` on engines or targets that cannot honor them.
- The app shell decides default destination paths; engines only receive resolved filesystem paths.

## Error Handling Rules

- Engines never return stack traces as user-facing messages.
- `message` is concise and safe for direct display.
- `details` may contain structured diagnostics for the diagnostics workspace.
- Unsupported target capabilities must return `unsupported_feature` with a `capabilityKey`.

## Temp and Download Path Rules

- Default download directory is the platform Downloads directory.
- Default temp directory is the platform temp directory.
- Settings may override both with absolute paths.
- The shell validates and creates missing directories before dispatching transfer requests.

## UI Expectations

- Unsupported features stay visible but disabled with an explanation.
- Workspace changes should preserve context when practical.
- The app must stay interactive while listing, transferring, or benchmarking.
- Long-running operations must surface progress and recovery actions.

