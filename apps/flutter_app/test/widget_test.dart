import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:s3_browser_crossplat/app/s3_browser_app.dart';
import 'package:s3_browser_crossplat/benchmark/benchmark_workspace.dart';
import 'package:s3_browser_crossplat/browser/browser_workspace.dart';
import 'package:s3_browser_crossplat/controllers/app_controller.dart';
import 'package:s3_browser_crossplat/models/domain_models.dart';
import 'package:s3_browser_crossplat/services/mock_engine_service.dart';

class TestAppController extends AppController {
  TestAppController({
    required super.engineService,
    required super.initialSettings,
    required super.initialProfiles,
  });

  void emitChange() {
    notifyListeners();
  }
}

Future<TestAppController> _buildController() async {
  final controller = TestAppController(
    engineService: MockEngineService(),
    initialSettings: const AppSettings(
      darkMode: false,
      defaultEngineId: 'rust',
      downloadPath: r'C:\Temp\downloads',
      tempPath: r'C:\Temp',
      transferConcurrency: 8,
      multipartThresholdMiB: 32,
      multipartChunkMiB: 8,
      enableAnimations: true,
      enableDiagnostics: true,
      enableApiLogging: false,
      enableDebugLogging: false,
      safeRetries: 3,
      benchmarkChartSmoothing: true,
      retryBaseDelayMs: 250,
      retryMaxDelayMs: 4000,
      requestDelayMs: 0,
      connectTimeoutSeconds: 5,
      readTimeoutSeconds: 60,
      maxPoolConnections: 200,
      maxRequestsPerSecond: 0,
      enableCrashRecovery: true,
      defaultPresignMinutes: 60,
      benchmarkDataCacheMb: 0,
      benchmarkDebugMode: false,
      benchmarkLogPath: r'C:\Temp\benchmark.log',
      browserInspectorLayout: BrowserInspectorLayout.bottom,
      browserInspectorSize: 360,
      uiScalePercent: 80,
      logTextScalePercent: 90,
    ),
    initialProfiles: const [
      EndpointProfile(
        id: 'test',
        name: 'Test',
        endpointUrl: 'http://localhost:9000',
        region: 'us-east-1',
        accessKey: 'key',
        secretKey: 'secret',
        pathStyle: true,
        verifyTls: false,
      ),
    ],
  );
  await controller.initialize();
  return controller;
}

BucketSummary _bucket(int index) {
  return BucketSummary(
    name: 'bucket-$index',
    region: 'us-east-1',
    objectCountHint: index * 10,
    versioningEnabled: index.isEven,
  );
}

void _seedBuckets(TestAppController controller, {required int count}) {
  final buckets = List.generate(count, _bucket);
  controller.buckets = buckets;
  controller.selectedBucket = buckets.first;
  controller.emitChange();
}

Widget _bucketPanelApp(TestAppController controller, {required Size size}) {
  return MaterialApp(
    home: MediaQuery(
      data: MediaQueryData(size: size),
      child: Scaffold(
        body: Padding(
          padding: const EdgeInsets.all(16),
          child: SizedBox(
            width: size.width > 700 ? 320 : size.width - 32,
            height: size.height - 32,
            child: BrowserBucketPanel(
              controller: controller,
              compact: size.width < 700,
              onCreateBucket: () {},
              onDeleteBucket: (_, {force = false}) async {},
              onEditBucketLifecycle: (_) async {},
              onEditBucketPolicy: (_) async {},
              onEditBucketEncryption: (_) async {},
              onEditBucketTags: (_) async {},
              onToggleBucketVersioning: (_, __) async {},
              onCopyBucket: (_) async {},
              inlineSpinnerBuilder: () => const SizedBox.shrink(),
              inlineStatBuilder: (label, value) => Text('$label: $value'),
            ),
          ),
        ),
      ),
    ),
  );
}

Widget _benchmarkApp(TestAppController controller) {
  return MaterialApp(
    home: Scaffold(
      body: BenchmarkWorkspace(controller: controller),
    ),
  );
}

Widget _browserApp(
  TestAppController controller, {
  required Size size,
  required bool compact,
}) {
  return MaterialApp(
    home: MediaQuery(
      data: MediaQueryData(size: size),
      child: Scaffold(
        body: BrowserWorkspace(
          controller: controller,
          compact: compact,
        ),
      ),
    ),
  );
}

Finder _debugSwitchFinder() {
  return find.byWidgetPredicate((widget) {
    if (widget is! SwitchListTile) {
      return false;
    }
    final title = widget.title;
    return title is Text && (title.data ?? '').toLowerCase().contains('debug');
  });
}

EventLogEntry _apiTraceEntry({
  required String phase,
  required String requestId,
  required DateTime timestamp,
  String? objectKey,
  int? latencyMs,
}) {
  return EventLogEntry(
    timestamp: timestamp,
    level: 'API',
    category: phase == 'send' ? 'EngineRequest' : 'EngineResponse',
    message: '$phase trace',
    profileId: 'test',
    bucketName: 'bucket-0',
    objectKey: objectKey,
    source: 'api',
    requestId: requestId,
    tracePhase: phase,
    engineId: 'rust',
    method: 'HeadObject',
    responseStatus: phase == 'response' ? 'ok' : null,
    latencyMs: latencyMs,
    traceHead: {
      'requestId': requestId,
      'phase': phase,
    },
    traceBody: {
      'bucket': 'bucket-0',
      if (objectKey != null) 'key': objectKey,
      'phase': phase,
    },
  );
}

void main() {
  testWidgets('app shell renders before deferred initialization completes', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = TestAppController(
      engineService: MockEngineService(),
      initialSettings: const AppSettings(
        darkMode: false,
        defaultEngineId: 'rust',
        downloadPath: r'C:\Temp\downloads',
        tempPath: r'C:\Temp',
        transferConcurrency: 8,
        multipartThresholdMiB: 32,
        multipartChunkMiB: 8,
        enableAnimations: true,
        enableDiagnostics: true,
        enableApiLogging: false,
        enableDebugLogging: false,
        safeRetries: 3,
        benchmarkChartSmoothing: true,
        retryBaseDelayMs: 250,
        retryMaxDelayMs: 4000,
        requestDelayMs: 0,
        connectTimeoutSeconds: 5,
        readTimeoutSeconds: 60,
        maxPoolConnections: 200,
        maxRequestsPerSecond: 0,
        enableCrashRecovery: true,
        defaultPresignMinutes: 60,
        benchmarkDataCacheMb: 0,
        benchmarkDebugMode: false,
        benchmarkLogPath: r'C:\Temp\benchmark.log',
        browserInspectorLayout: BrowserInspectorLayout.bottom,
        browserInspectorSize: 360,
        uiScalePercent: 80,
        logTextScalePercent: 90,
      ),
      initialProfiles: const [
        EndpointProfile(
          id: 'test',
          name: 'Test',
          endpointUrl: 'http://localhost:9000',
          region: 'us-east-1',
          accessKey: 'key',
          secretKey: 'secret',
          pathStyle: true,
          verifyTls: false,
        ),
      ],
    );

    expect(controller.engines, isEmpty);

    await tester.pumpWidget(S3BrowserApp(controller: controller));

    expect(find.text('S3 Browser Crossplat'), findsOneWidget);
  });

  testWidgets('app renders top-level workspaces', (WidgetTester tester) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1600, 1200));
    final controller = await _buildController();
    controller.activeTab = WorkspaceTab.settings;
    controller.emitChange();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    expect(find.text('S3 Browser Crossplat'), findsOneWidget);
    expect(find.text('Browser'), findsWidgets);
    expect(find.text('Benchmark'), findsWidgets);
    expect(find.text('Tasks'), findsWidgets);
    expect(find.text('Settings'), findsWidgets);
    expect(find.text('Event Log'), findsWidgets);
  });

  testWidgets('bucket list starts above profile summary in desktop layout', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = await _buildController();
    _seedBuckets(controller, count: 24);
    await tester.pumpWidget(
      _bucketPanelApp(controller, size: const Size(320, 900)),
    );
    await tester.pumpAndSettle();

    final createButton = find.widgetWithText(FilledButton, 'Create bucket');
    final firstBucket = find.text('bucket-0');
    final summary = find.byKey(const ValueKey('bucket-profile-summary'));

    expect(firstBucket, findsOneWidget);
    expect(summary, findsOneWidget);
    expect(find.text('Test'), findsOneWidget);
    expect(find.text('http://localhost:9000'), findsOneWidget);
    expect(
      tester.getTopLeft(firstBucket).dy,
      greaterThan(tester.getBottomLeft(createButton).dy),
    );
    expect(
      tester.getTopLeft(summary).dy,
      greaterThan(tester.getTopLeft(firstBucket).dy),
    );
  });

  testWidgets('bucket list scroll reaches the last bucket in desktop layout', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = await _buildController();
    _seedBuckets(controller, count: 40);
    await tester.pumpWidget(
      _bucketPanelApp(controller, size: const Size(320, 900)),
    );
    await tester.pumpAndSettle();

    final bucketList = find.byKey(const ValueKey('bucket-panel-scroll'));
    await tester.dragUntilVisible(
      find.text('bucket-39'),
      bucketList,
      const Offset(0, -240),
    );
    await tester.pumpAndSettle();

    expect(find.text('bucket-39'), findsOneWidget);
  });

  testWidgets('bucket list scroll reaches the last bucket in compact layout', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(680, 1280));

    final controller = await _buildController();
    _seedBuckets(controller, count: 40);
    await tester.pumpWidget(
      _bucketPanelApp(controller, size: const Size(680, 1280)),
    );
    await tester.pumpAndSettle();

    final bucketList = find.byKey(const ValueKey('bucket-panel-scroll'));
    await tester.dragUntilVisible(
      find.text('bucket-39'),
      bucketList,
      const Offset(0, -220),
    );
    await tester.pumpAndSettle();

    expect(find.text('bucket-39'), findsOneWidget);
  });

  testWidgets('tasks workspace renders top-level running task details', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = await _buildController();
    controller.browserTasks = [
      BrowserTaskRecord(
        id: 'upload-1',
        kind: BrowserTaskKind.transfer,
        label: 'Upload 1 file',
        status: 'running',
        startedAt: DateTime(2026, 3, 11, 10, 0),
        progress: 0.4,
        bucketName: 'bucket-0',
        strategyLabel: 'Multipart upload',
        itemCount: 2,
        itemsCompleted: 1,
        partsTotal: 4,
        partsCompleted: 2,
      ),
    ];
    controller.activeTab = WorkspaceTab.tasks;
    controller.emitChange();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    expect(find.text('Tasks'), findsWidgets);
    expect(find.text('Running'), findsWidgets);
    expect(find.text('Upload 1 file'), findsOneWidget);
    expect(find.text('Multipart upload'), findsOneWidget);
    expect(find.textContaining('Items: 1/2'), findsOneWidget);
    expect(find.textContaining('Parts: 2/4'), findsOneWidget);
  });

  testWidgets(
      'browser create prefix flow requires a name and updates the action label',
      (
    WidgetTester tester,
  ) async {
    final controller = await _buildController();

    await tester.pumpWidget(
      _browserApp(
        controller,
        size: const Size(1440, 1024),
        compact: false,
      ),
    );
    await tester.pumpAndSettle();

    expect(
        find.widgetWithText(OutlinedButton, 'Create prefix'), findsOneWidget);
    expect(find.text('Create folder'), findsNothing);

    await tester.tap(find.widgetWithText(OutlinedButton, 'Create prefix'));
    await tester.pumpAndSettle();

    expect(find.text('Create prefix'), findsWidgets);
    await tester.tap(find.widgetWithText(FilledButton, 'Create').last);
    await tester.pumpAndSettle();

    expect(find.byType(AlertDialog), findsOneWidget);

    await tester.enterText(
        find.widgetWithText(TextField, 'Prefix name'), 'reports/2026');
    await tester.tap(find.widgetWithText(FilledButton, 'Create').last);
    await tester.pumpAndSettle();

    expect(find.byType(AlertDialog), findsNothing);
  });

  testWidgets('settings workspace renders guided endpoint onboarding controls',
      (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = await _buildController();
    controller.activeTab = WorkspaceTab.settings;
    controller.emitChange();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    await tester.tap(find.byType(ExpansionTile).first);
    await tester.pumpAndSettle();

    expect(find.text('Endpoint type'), findsOneWidget);
    expect(find.text('Use HTTPS'), findsOneWidget);
    expect(find.text('Normalized endpoint'), findsOneWidget);
  });

  testWidgets(
      'browser workspace renders merged object filter and versions without object selection',
      (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));

    final controller = await _buildController();
    controller.selectedObject = null;
    controller.inspectorTab = BrowserInspectorTab.versions;
    controller.emitChange();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    expect(find.text('Filter mode'), findsAtLeastNWidgets(1));
    expect(find.text('Object filter (prefix)'), findsOneWidget);
    expect(find.text('Show all versions'), findsOneWidget);
    expect(find.text('Showing all versioned objects in the selected bucket.'),
        findsOneWidget);
  });

  testWidgets(
      'benchmark start honors typed duration without submitting the field', (
    WidgetTester tester,
  ) async {
    final controller = await _buildController();

    await tester.pumpWidget(_benchmarkApp(controller));
    await tester.pumpAndSettle();

    await tester.enterText(
      find.widgetWithText(TextFormField, 'Duration (s)'),
      '60',
    );
    await tester.tap(find.widgetWithText(FilledButton, 'Start benchmark'));
    await tester.pumpAndSettle();

    expect(controller.benchmarkRun, isNotNull);
    expect(controller.benchmarkRun!.config.durationSeconds, 60);
  });

  testWidgets('benchmark duration progress shows elapsed seconds of total', (
    WidgetTester tester,
  ) async {
    final controller = await _buildController();
    await controller.startBenchmark();
    final run = controller.benchmarkRun!.copyWith(
      status: 'running',
      activeElapsedSeconds: 42,
    );
    controller.benchmarkRun = run;
    controller.selectedBenchmarkRunId = run.id;
    controller.benchmarkHistory = [run];
    controller.emitChange();

    await tester.pumpWidget(_benchmarkApp(controller));
    await tester.pumpAndSettle();

    expect(find.text('42s of 120s'), findsOneWidget);
  });

  testWidgets('browser and benchmark screens no longer expose debug switches', (
    WidgetTester tester,
  ) async {
    final controller = await _buildController();
    controller.inspectorTab = BrowserInspectorTab.tools;
    controller.emitChange();

    await tester.pumpWidget(
      _browserApp(
        controller,
        size: const Size(1440, 1024),
        compact: false,
      ),
    );
    await tester.pumpAndSettle();
    expect(_debugSwitchFinder(), findsNothing);

    await tester.pumpWidget(_benchmarkApp(controller));
    await tester.pumpAndSettle();
    expect(_debugSwitchFinder(), findsNothing);
  });

  testWidgets(
      'profile dropdown uses readable text styling in light mode desktop and compact layouts',
      (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    final controller = await _buildController();

    await tester.binding.setSurfaceSize(const Size(1440, 1024));
    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    final desktopTexts = tester
        .widgetList<Text>(find.text('Test'))
        .where((text) => text.style?.color == Colors.black87);
    expect(desktopTexts, isNotEmpty);

    await tester.binding.setSurfaceSize(const Size(960, 1280));
    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    final compactTexts = tester
        .widgetList<Text>(find.text('Test'))
        .where((text) => text.style?.color == Colors.black87);
    expect(compactTexts, isNotEmpty);
  });

  testWidgets('event log groups API traces into expandable cards', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(1440, 1024));
    final controller = await _buildController();
    controller.activeTab = WorkspaceTab.eventLog;
    controller.eventLog = [
      _apiTraceEntry(
        phase: 'response',
        requestId: 'req-1',
        timestamp: DateTime(2026, 3, 22, 16, 0, 2),
        objectKey: 'backup-tool-v1.2.zip',
        latencyMs: 42,
      ),
      _apiTraceEntry(
        phase: 'send',
        requestId: 'req-1',
        timestamp: DateTime(2026, 3, 22, 16, 0, 1),
        objectKey: 'backup-tool-v1.2.zip',
      ),
      EventLogEntry(
        timestamp: DateTime(2026, 3, 22, 15, 59, 59),
        level: 'INFO',
        category: 'Settings',
        message: 'Updated application settings.',
      ),
    ];
    controller.emitChange();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    expect(find.text('HeadObject'), findsOneWidget);
    expect(find.text('Updated application settings.'), findsOneWidget);
    expect(find.text('Raw event text'), findsNothing);

    await tester.tap(find.text('HeadObject'));
    await tester.pumpAndSettle();

    expect(find.text('Send'), findsOneWidget);
    expect(find.text('Response'), findsOneWidget);
    expect(find.text('Raw event text'), findsOneWidget);
  });

  testWidgets('events and debug inspector renders grouped trace cards', (
    WidgetTester tester,
  ) async {
    final controller = await _buildController();
    controller.inspectorTab = BrowserInspectorTab.eventsAndDebug;
    controller.selectedObjectDetails = const ObjectDetails(
      key: 'backup-tool-v1.2.zip',
      metadata: {},
      headers: {},
      tags: {},
      debugEvents: [],
      apiCalls: [],
      debugLogExcerpt: ['Resolved endpoint'],
    );
    controller.eventLog = [
      _apiTraceEntry(
        phase: 'response',
        requestId: 'req-2',
        timestamp: DateTime(2026, 3, 22, 16, 10, 2),
        objectKey: 'backup-tool-v1.2.zip',
        latencyMs: 31,
      ),
      _apiTraceEntry(
        phase: 'send',
        requestId: 'req-2',
        timestamp: DateTime(2026, 3, 22, 16, 10, 1),
        objectKey: 'backup-tool-v1.2.zip',
      ),
    ];
    controller.emitChange();

    await tester.pumpWidget(
      _browserApp(
        controller,
        size: const Size(1440, 1024),
        compact: false,
      ),
    );
    await tester.pumpAndSettle();

    expect(find.text('Trace log'), findsOneWidget);
    expect(find.text('HeadObject'), findsOneWidget);
    expect(find.text('Debug excerpt'), findsOneWidget);
  });

  testWidgets('phone shell uses bottom navigation instead of top tabs', (
    WidgetTester tester,
  ) async {
    addTearDown(() => tester.binding.setSurfaceSize(null));
    await tester.binding.setSurfaceSize(const Size(390, 844));
    final controller = await _buildController();

    await tester.pumpWidget(S3BrowserApp(controller: controller));
    await tester.pumpAndSettle();

    expect(find.byType(NavigationBar), findsOneWidget);
    expect(find.byType(SegmentedButton<WorkspaceTab>), findsNothing);
  });
}
