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
      title: 'S3 Browser Crossplat',
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
              final phone = constraints.maxWidth < 700;
              final compact = constraints.maxWidth < 1200;
              final body = AnimatedSwitcher(
                duration: controller.settings.enableAnimations
                    ? const Duration(milliseconds: 280)
                    : Duration.zero,
                switchInCurve: Curves.easeOutCubic,
                switchOutCurve: Curves.easeInCubic,
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
                bottomNavigationBar: phone ? _buildBottomNav(controller) : null,
                body: DecoratedBox(
                  decoration: BoxDecoration(
                    gradient: LinearGradient(
                      begin: Alignment.topCenter,
                      end: Alignment.bottomCenter,
                      colors: [
                        Theme.of(context)
                            .colorScheme
                            .primaryContainer
                            .withValues(alpha: 0.18),
                        Theme.of(context).scaffoldBackgroundColor,
                        Theme.of(context)
                            .colorScheme
                            .secondaryContainer
                            .withValues(alpha: 0.12),
                      ],
                    ),
                  ),
                  child: SafeArea(
                    child: Row(
                      children: [
                        if (!compact) _buildRail(controller),
                        Expanded(
                          child: Column(
                            children: [
                              _AppHeader(
                                controller: controller,
                                compact: compact,
                                phone: phone,
                              ),
                              if (compact && !phone) _buildTopTabs(controller),
                              Expanded(child: body),
                            ],
                          ),
                        ),
                      ],
                    ),
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
      padding: const EdgeInsets.fromLTRB(18, 0, 18, 14),
      child: SegmentedButton<WorkspaceTab>(
        segments: const [
          ButtonSegment(value: WorkspaceTab.browser, label: Text('Browser')),
          ButtonSegment(
            value: WorkspaceTab.benchmark,
            label: Text('Benchmark'),
          ),
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

  Widget _buildBottomNav(AppController controller) {
    return NavigationBar(
      selectedIndex: WorkspaceTab.values.indexOf(controller.activeTab),
      onDestinationSelected: (index) =>
          controller.selectTab(WorkspaceTab.values[index]),
      destinations: const [
        NavigationDestination(
          icon: Icon(Icons.folder_open_outlined),
          selectedIcon: Icon(Icons.folder_open),
          label: 'Browser',
        ),
        NavigationDestination(
          icon: Icon(Icons.speed_outlined),
          selectedIcon: Icon(Icons.speed),
          label: 'Benchmark',
        ),
        NavigationDestination(
          icon: Icon(Icons.task_alt_outlined),
          selectedIcon: Icon(Icons.task_alt),
          label: 'Tasks',
        ),
        NavigationDestination(
          icon: Icon(Icons.tune_outlined),
          selectedIcon: Icon(Icons.tune),
          label: 'Settings',
        ),
        NavigationDestination(
          icon: Icon(Icons.receipt_long_outlined),
          selectedIcon: Icon(Icons.receipt_long),
          label: 'Event Log',
        ),
      ],
    );
  }
}

class _AppHeader extends StatelessWidget {
  const _AppHeader({
    required this.controller,
    required this.compact,
    required this.phone,
  });

  final AppController controller;
  final bool compact;
  final bool phone;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final desktopWide = !phone && !compact;
    return Container(
      margin: EdgeInsets.fromLTRB(phone ? 12 : 18, phone ? 6 : 18, phone ? 12 : 18, phone ? 8 : 10),
      padding: EdgeInsets.fromLTRB(18, compact ? 14 : 16, 18, 14),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(phone ? 28 : 30),
        gradient: LinearGradient(
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
          colors: [
            theme.colorScheme.surface,
            theme.colorScheme.primaryContainer.withValues(alpha: 0.34),
            theme.colorScheme.secondaryContainer.withValues(alpha: 0.22),
          ],
        ),
        border: Border.all(
          color: theme.colorScheme.outlineVariant.withValues(alpha: 0.75),
        ),
      ),
      child: TweenAnimationBuilder<double>(
        tween: Tween(begin: 0, end: 1),
        duration: controller.settings.enableAnimations
            ? const Duration(milliseconds: 360)
            : Duration.zero,
        curve: Curves.easeOutCubic,
        builder: (context, value, child) {
          return Transform.translate(
            offset: Offset(0, 12 * (1 - value)),
            child: Opacity(opacity: value, child: child),
          );
        },
        child: desktopWide
            ? Row(
                children: [
                  const Expanded(child: _DesktopHeaderMark()),
                  const SizedBox(width: 20),
                  ConstrainedBox(
                    constraints: const BoxConstraints(maxWidth: 300),
                    child: _HeaderControlStrip(
                      controller: controller,
                      embedded: true,
                      desktopPinned: true,
                    ),
                  ),
                ],
              )
            : _HeaderControlStrip(
                controller: controller,
                embedded: true,
                desktopPinned: !phone,
              ),
      ),
    );
  }
}

class _DesktopHeaderMark extends StatelessWidget {
  const _DesktopHeaderMark();

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Align(
      alignment: Alignment.centerLeft,
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Container(
            width: 60,
            height: 60,
            decoration: BoxDecoration(
              color: theme.colorScheme.primaryContainer.withValues(alpha: 0.95),
              borderRadius: BorderRadius.circular(20),
              border: Border.all(
                color: theme.colorScheme.outlineVariant.withValues(alpha: 0.85),
              ),
            ),
            child: Icon(
              Icons.storage_rounded,
              size: 30,
              color: theme.colorScheme.primary,
            ),
          ),
          const SizedBox(width: 14),
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
            decoration: BoxDecoration(
              color: theme.colorScheme.surface.withValues(alpha: 0.72),
              borderRadius: BorderRadius.circular(18),
              border: Border.all(
                color: theme.colorScheme.outlineVariant.withValues(alpha: 0.7),
              ),
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(
                  Icons.folder_copy_outlined,
                  size: 18,
                  color: theme.colorScheme.secondary,
                ),
                const SizedBox(width: 8),
                Text(
                  'Buckets • Objects • Inspect',
                  style: theme.textTheme.labelLarge?.copyWith(
                    color: theme.colorScheme.onSurface,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class _HeaderControlStrip extends StatelessWidget {
  const _HeaderControlStrip({
    required this.controller,
    this.embedded = false,
    this.desktopPinned = false,
  });

  final AppController controller;
  final bool embedded;
  final bool desktopPinned;

  @override
  Widget build(BuildContext context) {
    final profiles = controller.profiles;
    final engines = controller.engines;
    final phone = MediaQuery.sizeOf(context).width < 700;
    return Container(
      margin: embedded
          ? EdgeInsets.zero
          : phone
              ? const EdgeInsets.only(top: 2)
              : const EdgeInsets.fromLTRB(18, 0, 18, 14),
      padding: embedded
          ? EdgeInsets.zero
          : phone
              ? const EdgeInsets.only(top: 4)
              : const EdgeInsets.fromLTRB(18, 10, 18, 0),
      child: phone
          ? Column(
              children: [
                _labeledPhoneField(
                  context,
                  label: 'Endpoint profile',
                  child: _profileDropdown(context, profiles, phone: true),
                ),
                const SizedBox(height: 12),
                _labeledPhoneField(
                  context,
                  label: 'Backend engine',
                  child: _engineDropdown(context, engines, phone: true),
                ),
              ],
            )
          : Align(
              alignment: desktopPinned ? Alignment.topRight : Alignment.topLeft,
              child: desktopPinned
                  ? Column(
                      crossAxisAlignment: CrossAxisAlignment.end,
                      children: [
                        SizedBox(
                          width: compactWidth(context, embedded, true),
                          child:
                              _profileDropdown(context, profiles, phone: false),
                        ),
                        const SizedBox(height: 12),
                        SizedBox(
                          width: compactWidth(context, embedded, false),
                          child: _engineDropdown(context, engines, phone: false),
                        ),
                      ],
                    )
                  : Wrap(
                      spacing: 12,
                      runSpacing: 12,
                      children: [
                        SizedBox(
                          width: compactWidth(context, embedded, true),
                          child: _profileDropdown(
                            context,
                            profiles,
                            phone: false,
                          ),
                        ),
                        SizedBox(
                          width: compactWidth(context, embedded, false),
                          child: _engineDropdown(
                            context,
                            engines,
                            phone: false,
                          ),
                        ),
                      ],
                    ),
            ),
    );
  }

  double compactWidth(BuildContext context, bool embedded, bool profile) {
    if (desktopPinned) {
      return embedded ? 270 : 250;
    }
    return profile
        ? (embedded ? 320 : 280)
        : (embedded ? 240 : 220);
  }

  Widget _labeledPhoneField(
    BuildContext context, {
    required String label,
    required Widget child,
  }) {
    final theme = Theme.of(context);
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Padding(
          padding: const EdgeInsets.only(left: 4, bottom: 6),
          child: Text(
            label,
            style: theme.textTheme.labelLarge?.copyWith(
              fontWeight: FontWeight.w700,
              color: theme.colorScheme.onSurfaceVariant,
              letterSpacing: 0.2,
            ),
          ),
        ),
        child,
      ],
    );
  }

  Widget _profileDropdown(
    BuildContext context,
    List<EndpointProfile> profiles, {
    required bool phone,
  }) {
    final onSurface = phone ? Theme.of(context).colorScheme.onSurface : null;
    return DropdownButtonFormField<String>(
      initialValue: controller.selectedProfile?.id,
      isExpanded: true,
      dropdownColor: Theme.of(context).colorScheme.surface,
      style: Theme.of(context).textTheme.bodyMedium?.copyWith(
            color: onSurface,
          ),
      decoration: InputDecoration(
        labelText: phone ? null : 'Endpoint profile',
        hintText: profiles.isEmpty ? 'Create a profile in Settings' : null,
      ),
      items: profiles
          .map(
            (profile) => DropdownMenuItem(
              value: profile.id,
              child: Text(profile.name),
            ),
          )
          .toList(),
      onChanged: profiles.isEmpty
          ? null
          : (value) {
              if (value != null) {
                controller.setSelectedProfileById(value);
              }
            },
    );
  }

  Widget _engineDropdown(
    BuildContext context,
    List<EngineDescriptor> engines, {
    required bool phone,
  }) {
    return DropdownButtonFormField<String>(
      initialValue: controller.activeEngineId,
      isExpanded: true,
      decoration: InputDecoration(labelText: phone ? null : 'Backend engine'),
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
