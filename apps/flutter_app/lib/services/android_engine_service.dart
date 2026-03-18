import 'dart:async';

import 'package:flutter/services.dart';

import '../models/domain_models.dart';
import 'engine_service.dart';
import 'mock_engine_service.dart';

class AndroidEngineService implements EngineService, TransferJobSinkRegistrant {
  AndroidEngineService({
    MethodChannel? channel,
    EngineService? fallback,
  })  : _channel = channel ??
            const MethodChannel('s3_browser_crossplat/android_engine'),
        _fallback = fallback ?? MockEngineService();

  final MethodChannel _channel;
  final EngineService _fallback;

  @override
  void configureDiagnostics(DiagnosticsOptions options) {
    _fallback.configureDiagnostics(options);
  }

  @override
  void setTransferSink(TransferJobCallback? sink) {
    if (_fallback is TransferJobSinkRegistrant) {
      (_fallback as TransferJobSinkRegistrant).setTransferSink(sink);
    }
  }

  @override
  Future<List<EngineDescriptor>> listEngines() async {
    try {
      final items =
          await _channel.invokeListMethod<Map<dynamic, dynamic>>('listEngines');
      if (items == null || items.isEmpty) {
        return _fallback.listEngines();
      }
      return items.map((entry) {
        final value = Map<String, Object?>.from(entry.cast<String, Object?>());
        return EngineDescriptor(
          id: value['id'] as String? ?? 'android',
          label: value['label'] as String? ?? 'Android Engine',
          language: value['language'] as String? ?? 'native',
          version: value['version'] as String? ?? '2.0.8',
          available: value['available'] as bool? ?? true,
          desktopSupported: false,
          androidSupported: true,
        );
      }).toList(growable: false);
    } on PlatformException {
      return _fallback.listEngines();
    } on MissingPluginException {
      return _fallback.listEngines();
    }
  }

  @override
  Future<List<CapabilityDescriptor>> getCapabilities({
    required String engineId,
    required EndpointProfile profile,
  }) async {
    try {
      final result = await _channel.invokeMapMethod<String, Object?>(
        'dispatch',
        <String, Object?>{
          'engineId': engineId,
          'method': 'getCapabilities',
          'params': _profileEnvelope(profile),
        },
      );
      final items = (result?['items'] as List<Object?>? ?? const <Object?>[]);
      if (items.isEmpty) {
        return _fallback.getCapabilities(engineId: engineId, profile: profile);
      }
      return items.map((item) {
        final value = Map<String, Object?>.from(item as Map);
        return CapabilityDescriptor(
          key: value['key'] as String? ?? '',
          label: value['label'] as String? ?? '',
          state: switch (value['state']) {
            'supported' => CapabilityState.supported,
            'unsupported' => CapabilityState.unsupported,
            _ => CapabilityState.unknown,
          },
          reason: value['reason'] as String?,
        );
      }).toList(growable: false);
    } on PlatformException {
      return _fallback.getCapabilities(engineId: engineId, profile: profile);
    } on MissingPluginException {
      return _fallback.getCapabilities(engineId: engineId, profile: profile);
    }
  }

  @override
  Future<void> testProfile({
    required String engineId,
    required EndpointProfile profile,
  }) {
    return _fallback.testProfile(engineId: engineId, profile: profile);
  }

  @override
  Future<List<BucketSummary>> listBuckets({
    required String engineId,
    required EndpointProfile profile,
  }) {
    return _fallback.listBuckets(engineId: engineId, profile: profile);
  }

  @override
  Future<BucketSummary> createBucket({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required bool enableVersioning,
    required bool enableObjectLock,
  }) {
    return _fallback.createBucket(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      enableVersioning: enableVersioning,
      enableObjectLock: enableObjectLock,
    );
  }

  @override
  Future<void> deleteBucket({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucket(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> getBucketAdminState({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.getBucketAdminState(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> setBucketVersioning({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required bool enabled,
  }) {
    return _fallback.setBucketVersioning(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      enabled: enabled,
    );
  }

  @override
  Future<BucketAdminState> putBucketLifecycle({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String lifecycleJson,
  }) {
    return _fallback.putBucketLifecycle(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      lifecycleJson: lifecycleJson,
    );
  }

  @override
  Future<BucketAdminState> deleteBucketLifecycle({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucketLifecycle(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> putBucketPolicy({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String policyJson,
  }) {
    return _fallback.putBucketPolicy(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      policyJson: policyJson,
    );
  }

  @override
  Future<BucketAdminState> deleteBucketPolicy({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucketPolicy(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> putBucketCors({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String corsJson,
  }) {
    return _fallback.putBucketCors(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      corsJson: corsJson,
    );
  }

  @override
  Future<BucketAdminState> deleteBucketCors({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucketCors(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> putBucketEncryption({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String encryptionJson,
  }) {
    return _fallback.putBucketEncryption(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      encryptionJson: encryptionJson,
    );
  }

  @override
  Future<BucketAdminState> deleteBucketEncryption({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucketEncryption(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<BucketAdminState> putBucketTagging({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required Map<String, String> tags,
  }) {
    return _fallback.putBucketTagging(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      tags: tags,
    );
  }

  @override
  Future<BucketAdminState> deleteBucketTagging({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
  }) {
    return _fallback.deleteBucketTagging(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
    );
  }

  @override
  Future<ObjectListResult> listObjects({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String prefix,
    required bool flat,
    ListCursor? cursor,
  }) {
    return _fallback.listObjects(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      prefix: prefix,
      flat: flat,
      cursor: cursor,
    );
  }

  @override
  Future<ObjectVersionListResult> listObjectVersions({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    String? key,
    VersionBrowserOptions? options,
    ListCursor? cursor,
  }) {
    return _fallback.listObjectVersions(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      key: key,
      options: options,
      cursor: cursor,
    );
  }

  @override
  Future<ObjectDetails> getObjectDetails({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String key,
  }) {
    return _fallback.getObjectDetails(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      key: key,
    );
  }

  @override
  Future<void> createFolder({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String key,
  }) {
    return _fallback.createFolder(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      key: key,
    );
  }

  @override
  Future<BatchOperationResult> copyObject({
    required String engineId,
    required EndpointProfile profile,
    required String sourceBucketName,
    required String sourceKey,
    required String destinationBucketName,
    required String destinationKey,
  }) {
    return _fallback.copyObject(
      engineId: engineId,
      profile: profile,
      sourceBucketName: sourceBucketName,
      sourceKey: sourceKey,
      destinationBucketName: destinationBucketName,
      destinationKey: destinationKey,
    );
  }

  @override
  Future<BatchOperationResult> moveObject({
    required String engineId,
    required EndpointProfile profile,
    required String sourceBucketName,
    required String sourceKey,
    required String destinationBucketName,
    required String destinationKey,
  }) {
    return _fallback.moveObject(
      engineId: engineId,
      profile: profile,
      sourceBucketName: sourceBucketName,
      sourceKey: sourceKey,
      destinationBucketName: destinationBucketName,
      destinationKey: destinationKey,
    );
  }

  @override
  Future<BatchOperationResult> deleteObjects({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required List<String> keys,
  }) {
    return _fallback.deleteObjects(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      keys: keys,
    );
  }

  @override
  Future<BatchOperationResult> deleteObjectVersions({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required List<ObjectVersionRef> versions,
  }) {
    return _fallback.deleteObjectVersions(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      versions: versions,
    );
  }

  @override
  Future<TransferJob> startUpload({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String prefix,
    required List<String> filePaths,
    required int multipartThresholdMiB,
    required int multipartChunkMiB,
  }) {
    return _fallback.startUpload(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      prefix: prefix,
      filePaths: filePaths,
      multipartThresholdMiB: multipartThresholdMiB,
      multipartChunkMiB: multipartChunkMiB,
    );
  }

  @override
  Future<TransferJob> startDownload({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required List<String> keys,
    required String destinationPath,
    required int multipartThresholdMiB,
    required int multipartChunkMiB,
  }) {
    return _fallback.startDownload(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      keys: keys,
      destinationPath: destinationPath,
      multipartThresholdMiB: multipartThresholdMiB,
      multipartChunkMiB: multipartChunkMiB,
    );
  }

  @override
  Future<TransferJob> pauseTransfer({
    required String engineId,
    required String jobId,
  }) {
    return _fallback.pauseTransfer(engineId: engineId, jobId: jobId);
  }

  @override
  Future<TransferJob> resumeTransfer({
    required String engineId,
    required String jobId,
  }) {
    return _fallback.resumeTransfer(engineId: engineId, jobId: jobId);
  }

  @override
  Future<TransferJob> cancelTransfer({
    required String engineId,
    required String jobId,
  }) {
    return _fallback.cancelTransfer(engineId: engineId, jobId: jobId);
  }

  @override
  Future<String> generatePresignedUrl({
    required String engineId,
    required EndpointProfile profile,
    required String bucketName,
    required String key,
    required Duration expiration,
  }) {
    return _fallback.generatePresignedUrl(
      engineId: engineId,
      profile: profile,
      bucketName: bucketName,
      key: key,
      expiration: expiration,
    );
  }

  @override
  Future<ToolExecutionState> runPutTestData({
    required String engineId,
    required EndpointProfile profile,
    required TestDataToolConfig config,
  }) {
    return _fallback.runPutTestData(
      engineId: engineId,
      profile: profile,
      config: config,
    );
  }

  @override
  Future<ToolExecutionState> runDeleteAll({
    required String engineId,
    required EndpointProfile profile,
    required DeleteAllToolConfig config,
  }) {
    return _fallback.runDeleteAll(
      engineId: engineId,
      profile: profile,
      config: config,
    );
  }

  @override
  Future<ToolExecutionState> cancelToolExecution({
    required String engineId,
    required String jobId,
  }) {
    return _fallback.cancelToolExecution(engineId: engineId, jobId: jobId);
  }

  @override
  Future<BenchmarkRun> startBenchmark({
    required BenchmarkConfig config,
    required EndpointProfile profile,
  }) {
    return _fallback.startBenchmark(config: config, profile: profile);
  }

  @override
  Future<BenchmarkRun> getBenchmarkStatus(String runId) {
    return _fallback.getBenchmarkStatus(runId);
  }

  @override
  Future<void> pauseBenchmark(String runId) {
    return _fallback.pauseBenchmark(runId);
  }

  @override
  Future<void> resumeBenchmark(String runId) {
    return _fallback.resumeBenchmark(runId);
  }

  @override
  Future<void> stopBenchmark(String runId) {
    return _fallback.stopBenchmark(runId);
  }

  @override
  Future<BenchmarkExportBundle> exportBenchmarkResults({
    required String runId,
    required String format,
  }) {
    return _fallback.exportBenchmarkResults(runId: runId, format: format);
  }

  static Map<String, Object?> _profileEnvelope(EndpointProfile profile) {
    return <String, Object?>{
      'profile': <String, Object?>{
        'id': profile.id,
        'name': profile.name,
        'endpointUrl': profile.endpointUrl,
        'region': profile.region,
        'endpointType': profile.endpointType.name,
        'accessKey': profile.accessKey,
        'secretKey': profile.secretKey,
        'sessionToken': profile.sessionToken,
        'pathStyle': profile.pathStyle,
        'verifyTls': profile.verifyTls,
        'maxAttempts': profile.maxAttempts,
        'maxConcurrentRequests': profile.maxConcurrentRequests,
      },
    };
  }
}
