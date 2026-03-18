# Transport Contract

## Desktop

- Request/response: line-delimited JSON on stdin/stdout
- Progress/events: line-delimited JSON on stdout with `event`
- Transfer progress events use `{"event":"transferProgress","job":{...}}` with the same transfer job shape returned by the final response
- Fatal diagnostics: stderr
- Process contract: exit code `0` on graceful shutdown, non-zero on engine failure

## Android

- Logical parity with desktop methods
- Native adapters translate platform-channel or FFI calls into the same method names and payload shapes

## Versioning

- Initial contract version: `1.0`
- Breaking request or response changes require a contract version bump
