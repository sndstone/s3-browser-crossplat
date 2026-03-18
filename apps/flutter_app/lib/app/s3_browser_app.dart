import 'dart:async';

import 'package:flutter/material.dart';

import '../benchmark/benchmark_workspace.dart';
import '../browser/browser_workspace.dart';
import '../controllers/app_controller.dart';
import '../event_log/event_log_workspace.dart';
import '../models/domain_models.dart';
import '../settings/settings_workspace.dart';
import '../tasks/tasks_workspace.dart';
import '../theme/app_theme.dart';

class S3BrowserApp extends StatefulWidget {
  const S3BrowserApp({
    super.key,
    required this.controller,
  });

  final AppController controller;

  @override
  State<S3BrowserApp> createState() => _S3BrowserAppState();
}

class _S3BrowserAppState extends State<S3BrowserApp> {
  Timer? _benchmarkTimer;

  @override
  void initState() {
    super.initState();
    widget.controller.addListener(_handleControllerChange);
    WidgetsBinding.instance.addPostFrameCallback((_) {
      widget.controller.initialize();
    });
  }

  @override
  void dispose() {
    _benchmarkTimer?.cancel();
    widget.controller.removeListener(_handleControllerChange);
    super.dispose();
  }

  void _handleControllerChange() {
    final run = widget.controller.benchmarkRun;
    if (run != null && run.status == 'running') {
      _benchmarkTimer ??= Timer.periodic(const Duration(seconds: 1), (_) {
        widget.controller.pollBenchmark();
      });
    } else {
      _benchmarkTimer?.cancel();
      _benchmarkTimer = null;
    }

    if (mounted) {
      setState(() {});
    }
  }

  @override
  Widget build(BuildContext context) {
    final controller = widget.controller;
    final theme = controller.settings.darkMode
        ? AppTheme.dark(scalePercent: controller.settings.uiScalePercent)
        : AppTheme.light(scalePercent: controller.settings.uiScalePercent);

    return MaterialApp(
      title: 'S3 Browser Cross Platform',
      debugShowCheckedModeBanner: false,
      theme: theme,
      home: Builder(
        builder: (context) => MediaQuery(
          data: MediaQuery.of(context).copyWith(
            textScaler:
                TextScaler.linear(controller.settings.uiScalePercent / 100),
          ),
          child: LayoutBuilder(
            builder: (context, constraints) {
              final compact = constraints.maxWidth < 1200;
              final body = AnimatedSwitcher(
                duration: const Duration(milliseconds: 220),
                child: switch (controller.activeTab) {
                  WorkspaceTab.browser => BrowserWorkspace(
                      key: const ValueKey('browser'),
                      controller: controller,
                      compact: compact,
                    ),
                  WorkspaceTab.benchmark => BenchmarkWorkspace(
                      key: const ValueKey('benchmark'),
                      controller: controller,
                    ),
                  WorkspaceTab.settings => SettingsWorkspace(
                      key: const ValueKey('settings'),
                      controller: controller,
                    ),
                  WorkspaceTab.tasks => TasksWorkspace(
                      key: const ValueKey('tasks'),
                      controller: controller,
                    ),
                  WorkspaceTab.eventLog => EventLogWorkspace(
                      key: const ValueKey('event-log'),
                      controller: controller,
                    ),
                },
              );

              return Scaffold(
                body: SafeArea(
                  child: Row(
                    children: [
                      if (!compact) _buildRail(controller),
                      Expanded(
                        child: Column(
                          children: [
                            _AppHeader(
                                controller: controller, compact: compact),
                            if (compact) _buildTopTabs(controller),
                            Expanded(child: body),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
              );
            },
          ),
        ),
      ),
    );
  }

  Widget _buildRail(AppController controller) {
    return NavigationRail(
      selectedIndex: WorkspaceTab.values.indexOf(controller.activeTab),
      onDestinationSelected: (index) =>
          controller.selectTab(WorkspaceTab.values[index]),
      labelType: NavigationRailLabelType.all,
      destinations: const [
        NavigationRailDestination(
          icon: Icon(Icons.folder_open_outlined),
          selectedIcon: Icon(Icons.folder_open),
          label: Text('Browser'),
        ),
        NavigationRailDestination(
          icon: Icon(Icons.speed_outlined),
          selectedIcon: Icon(Icons.speed),
          label: Text('Benchmark'),
        ),
        NavigationRailDestination(
          icon: Icon(Icons.task_alt_outlined),
          selectedIcon: Icon(Icons.task_alt),
          label: Text('Tasks'),
        ),
        NavigationRailDestination(
          icon: Icon(Icons.tune_outlined),
          selectedIcon: Icon(Icons.tune),
          label: Text('Settings'),
        ),
        NavigationRailDestination(
          icon: Icon(Icons.receipt_long_outlined),
          selectedIcon: Icon(Icons.receipt_long),
          label: Text('Event Log'),
        ),
      ],
    );
  }

  Widget _buildTopTabs(AppController controller) {
    return Padding(
      padding: const EdgeInsets.all(12),
      child: SegmentedButton<WorkspaceTab>(
        segments: const [
          ButtonSegment(value: WorkspaceTab.browser, label: Text('Browser')),
          ButtonSegment(
              value: WorkspaceTab.benchmark, label: Text('Benchmark')),
          ButtonSegment(value: WorkspaceTab.tasks, label: Text('Tasks')),
          ButtonSegment(value: WorkspaceTab.settings, label: Text('Settings')),
          ButtonSegment(value: WorkspaceTab.eventLog, label: Text('Event Log')),
        ],
        selected: {controller.activeTab},
        onSelectionChanged: (selection) =>
            controller.selectTab(selection.first),
      ),
    );
  }
}

class _AppHeader extends StatelessWidget {
  const _AppHeader({
    required this.controller,
    required this.compact,
  });

  final AppController controller;
  final bool compact;

  @override
  Widget build(BuildContext context) {
    final engines = controller.engines;
    final profiles = controller.profiles;

    return Container(
      padding: const EdgeInsets.fromLTRB(20, 16, 20, 12),
      decoration: BoxDecoration(
        gradient: LinearGradient(
          colors: [
            Theme.of(context).colorScheme.surface,
            Theme.of(context).colorScheme.surfaceContainerHighest,
          ],
        ),
      ),
      child: compact
          ? Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                _title(context),
                const SizedBox(height: 12),
                Wrap(
                  spacing: 12,
                  runSpacing: 12,
                  children: [
                    SizedBox(
                      width: 240,
                      child: _profileDropdown(context, profiles),
                    ),
                    SizedBox(width: 200, child: _engineDropdown(engines)),
                  ],
                ),
              ],
            )
          : Row(
              children: [
                Expanded(child: _title(context)),
                SizedBox(
                  width: 260,
                  child: _profileDropdown(context, profiles),
                ),
                const SizedBox(width: 12),
                SizedBox(width: 220, child: _engineDropdown(engines)),
              ],
            ),
    );
  }

  Widget _title(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'S3 Browser Cross Platform',
          style: Theme.of(context).textTheme.headlineMedium,
        ),
        Text(
          'Unified browser and benchmark workspace for AWS S3 and S3-compatible targets',
          style: Theme.of(context).textTheme.bodyMedium,
        ),
      ],
    );
  }

  Widget _profileDropdown(
    BuildContext context,
    List<EndpointProfile> profiles,
  ) {
    final onSurface =
        controller.settings.darkMode ? Colors.white : Colors.black87;
    return DropdownButtonFormField<String>(
      initialValue: controller.selectedProfile?.id,
      isExpanded: true,
      dropdownColor: Theme.of(context).colorScheme.surface,
      style: Theme.of(context).textTheme.bodyMedium?.copyWith(
            color: onSurface,
          ),
      decoration: InputDecoration(
        labelText: 'Endpoint profile',
        hintText: profiles.isEmpty ? 'Create a profile in Settings' : null,
      ),
      items: profiles
          .map(
            (profile) => DropdownMenuItem(
              value: profile.id,
              child: Text(
                profile.name,
                style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                      color: onSurface,
                    ),
              ),
            ),
          )
          .toList(),
      onChanged: profiles.isEmpty
          ? null
          : (value) {
              if (value == null) {
                return;
              }
              controller.setSelectedProfileById(value);
            },
    );
  }

  Widget _engineDropdown(List<EngineDescriptor> engines) {
    return DropdownButtonFormField<String>(
      initialValue: controller.activeEngineId,
      isExpanded: true,
      decoration: const InputDecoration(labelText: 'Backend engine'),
      items: engines
          .map(
            (engine) => DropdownMenuItem(
              value: engine.id,
              child: Text(engine.label),
            ),
          )
          .toList(),
      onChanged: (value) {
        if (value != null) {
          controller.setEngine(value);
        }
      },
    );
  }
}
