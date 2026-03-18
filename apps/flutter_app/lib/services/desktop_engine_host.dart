import 'dart:async';
import 'dart:convert';
import 'dart:io';

class DesktopEngineHostResponse {
  const DesktopEngineHostResponse({
    required this.payload,
    required this.stdoutOutput,
    required this.stderrOutput,
  });

  final Map<String, Object?> payload;
  final String stdoutOutput;
  final String stderrOutput;
}

class DesktopEngineHost {
  const DesktopEngineHost();

  Future<DesktopEngineHostResponse> send({
    required String executablePath,
    List<String> arguments = const [],
    String? workingDirectory,
    required Map<String, Object?> request,
    void Function(Map<String, Object?> event)? onEvent,
  }) async {
    final process = await Process.start(
      executablePath,
      arguments,
      workingDirectory: workingDirectory,
      runInShell:
          Platform.isWindows && executablePath.toLowerCase().endsWith('.bat'),
    );
    process.stdin.writeln(jsonEncode(request));
    await process.stdin.flush();
    await process.stdin.close();

    final stderrFuture = process.stderr.transform(utf8.decoder).join();
    final stdoutLines = <String>[];
    Map<String, Object?>? payload;
    final stdoutDone = Completer<void>();
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(
      (rawLine) {
        final line = rawLine.trim();
        if (line.isEmpty) {
          return;
        }
        stdoutLines.add(line);
        try {
          final decoded = jsonDecode(line);
          if (decoded is! Map) {
            return;
          }
          final message = Map<String, Object?>.from(decoded);
          if (message.containsKey('event')) {
            onEvent?.call(message);
            return;
          }
          if (message.containsKey('ok')) {
            payload ??= message;
          }
        } on FormatException {
          // Preserve non-JSON stdout in stdoutOutput for debugging.
        }
      },
      onDone: stdoutDone.complete,
      onError: stdoutDone.completeError,
      cancelOnError: true,
    );

    final exitCode = await process.exitCode;
    await stdoutDone.future;
    final stderrOutput = (await stderrFuture).trim();
    final trimmedStdout = stdoutLines.join('\n').trim();
    if (exitCode != 0) {
      throw ProcessException(
        executablePath,
        arguments,
        stderrOutput.isEmpty
            ? 'Engine exited with code $exitCode'
            : stderrOutput.trim(),
        exitCode,
      );
    }

    final lines = const LineSplitter()
        .convert(trimmedStdout)
        .where((line) => line.trim().isNotEmpty)
        .toList();
    if (lines.isEmpty) {
      throw ProcessException(
        executablePath,
        arguments,
        stderrOutput.isEmpty
            ? 'Engine exited without returning a response.'
            : stderrOutput,
        exitCode,
      );
    }
    final resolvedPayload = payload;
    if (resolvedPayload == null) {
      throw ProcessException(
        executablePath,
        arguments,
        stderrOutput.isEmpty
            ? 'Engine exited without returning a structured response.'
            : stderrOutput,
        exitCode,
      );
    }

    return DesktopEngineHostResponse(
      payload: resolvedPayload,
      stdoutOutput: trimmedStdout,
      stderrOutput: stderrOutput,
    );
  }
}
