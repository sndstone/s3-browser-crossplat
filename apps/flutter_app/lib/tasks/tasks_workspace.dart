import 'package:flutter/material.dart';

import '../controllers/app_controller.dart';
import '../models/domain_models.dart';

class TasksWorkspace extends StatelessWidget {
  const TasksWorkspace({
    super.key,
    required this.controller,
  });

  final AppController controller;

  @override
  Widget build(BuildContext context) {
    return DefaultTabController(
      length: 3,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(20),
              decoration: BoxDecoration(
                borderRadius: BorderRadius.circular(30),
                gradient: LinearGradient(
                  begin: Alignment.topLeft,
                  end: Alignment.bottomRight,
                  colors: [
                    Theme.of(context).colorScheme.surface,
                    Theme.of(context)
                        .colorScheme
                        .primaryContainer
                        .withValues(alpha: 0.38),
                    Theme.of(context)
                        .colorScheme
                        .secondaryContainer
                        .withValues(alpha: 0.2),
                  ],
                ),
                border: Border.all(
                  color: Theme.of(context).colorScheme.outlineVariant,
                ),
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Task stream',
                    style: Theme.of(context).textTheme.headlineSmall,
                  ),
                  const SizedBox(height: 8),
                  Text(
                    'Track uploads, downloads, bucket operations, tools, and benchmark runs from one queue.',
                    style: Theme.of(context).textTheme.bodyLarge,
                  ),
                ],
              ),
            ),
            const SizedBox(height: 16),
            Container(
              padding: const EdgeInsets.all(6),
              decoration: BoxDecoration(
                color: Theme.of(context)
                    .colorScheme
                    .surface
                    .withValues(alpha: 0.82),
                borderRadius: BorderRadius.circular(999),
                border: Border.all(
                  color: Theme.of(context).colorScheme.outlineVariant,
                ),
              ),
              child: TabBar(
                isScrollable: true,
                overlayColor: const WidgetStatePropertyAll(Colors.transparent),
                splashFactory: NoSplash.splashFactory,
                splashBorderRadius: BorderRadius.circular(999),
                tabAlignment: TabAlignment.start,
                labelPadding: const EdgeInsets.symmetric(horizontal: 18),
                tabs: const [
                  Tab(text: 'Running'),
                  Tab(text: 'Failed'),
                  Tab(text: 'All'),
                ],
              ),
            ),
            const SizedBox(height: 16),
            Expanded(
              child: TabBarView(
                children: [
                  _TaskList(
                    controller: controller,
                    view: BrowserTaskView.running,
                  ),
                  _TaskList(
                    controller: controller,
                    view: BrowserTaskView.failed,
                  ),
                  _TaskList(
                    controller: controller,
                    view: BrowserTaskView.all,
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _TaskList extends StatelessWidget {
  const _TaskList({
    required this.controller,
    required this.view,
  });

  final AppController controller;
  final BrowserTaskView view;

  @override
  Widget build(BuildContext context) {
    final tasks = controller.tasksForView(view);
    if (tasks.isEmpty) {
      return Center(
        child: Text(
          switch (view) {
            BrowserTaskView.running => 'No running tasks.',
            BrowserTaskView.failed => 'No failed tasks.',
            BrowserTaskView.all => 'No task history yet.',
          },
        ),
      );
    }

    return ListView.separated(
      itemCount: tasks.length,
      separatorBuilder: (_, __) => const SizedBox(height: 12),
      itemBuilder: (context, index) => _TaskCard(
        controller: controller,
        task: tasks[index],
      ),
    );
  }
}

class _TaskCard extends StatelessWidget {
  const _TaskCard({
    required this.controller,
    required this.task,
  });

  final AppController controller;
  final BrowserTaskRecord task;

  @override
  Widget build(BuildContext context) {
    final details = <String>[
      if (task.profileId != null) 'Profile: ${task.profileId}',
      if (task.bucketName != null) 'Bucket: ${task.bucketName}',
      'Started: ${_formatDateTime(task.startedAt)}',
      if (task.completedAt != null)
        'Completed: ${_formatDateTime(task.completedAt!)}',
      if (task.strategyLabel != null) 'Strategy: ${task.strategyLabel}',
      if (task.currentItemLabel != null)
        'Current item: ${task.currentItemLabel}',
    ];
    final metricLines = <String>[
      if (task.bytesTransferred != null && task.totalBytes != null)
        'Bytes: ${_formatBytes(task.bytesTransferred!)}/${_formatBytes(task.totalBytes!)}',
      if (task.itemCount != null)
        'Items: ${(task.itemsCompleted ?? 0)}/${task.itemCount}',
      if (task.partsTotal != null)
        'Parts: ${(task.partsCompleted ?? 0)}/${task.partsTotal}'
            '${task.partSizeBytes == null ? '' : ' • ${_formatBytes(task.partSizeBytes!)} per part'}',
    ];

    return Card(
      margin: EdgeInsets.zero,
      child: ExpansionTile(
        tilePadding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
        childrenPadding: const EdgeInsets.fromLTRB(16, 0, 16, 16),
        title: Text(
          task.label,
          style: Theme.of(context).textTheme.titleMedium,
        ),
        subtitle: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const SizedBox(height: 8),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                Chip(label: Text(task.status)),
                if (task.strategyLabel != null)
                  Chip(label: Text(task.strategyLabel!)),
                Chip(label: Text(task.kind.name)),
              ],
            ),
            const SizedBox(height: 8),
            LinearProgressIndicator(
              value: task.progress == 0 ? null : task.progress,
            ),
            const SizedBox(height: 8),
            if (metricLines.isNotEmpty)
              Text(
                metricLines.join('\n'),
                style: Theme.of(context).textTheme.bodySmall,
              ),
          ],
        ),
        children: [
          if (details.isNotEmpty)
            Align(
              alignment: Alignment.centerLeft,
              child: Text(details.join('\n')),
            ),
          if (details.isNotEmpty) const SizedBox(height: 12),
          if (task.outputLines.isNotEmpty)
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: Theme.of(context).colorScheme.surfaceContainerHighest,
                borderRadius: BorderRadius.circular(12),
              ),
              child: SelectableText(task.outputLines.join('\n')),
            ),
          if (task.outputLines.isNotEmpty) const SizedBox(height: 12),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              if (task.kind == BrowserTaskKind.transfer)
                OutlinedButton(
                  onPressed: task.canPause
                      ? () => controller.pauseTransfer(task.id)
                      : null,
                  child: const Text('Pause'),
                ),
              if (task.kind == BrowserTaskKind.transfer)
                OutlinedButton(
                  onPressed: task.canResume
                      ? () => controller.resumeTransfer(task.id)
                      : null,
                  child: const Text('Resume'),
                ),
              if (task.kind == BrowserTaskKind.transfer)
                OutlinedButton(
                  onPressed: task.canCancel
                      ? () => controller.cancelTransfer(task.id)
                      : null,
                  child: const Text('Cancel'),
                ),
              if (task.kind == BrowserTaskKind.benchmark)
                OutlinedButton(
                  onPressed: () => controller.selectTab(WorkspaceTab.benchmark),
                  child: const Text('Open benchmark'),
                ),
              if (task.kind == BrowserTaskKind.action &&
                  task.workspaceTab != null)
                OutlinedButton(
                  onPressed: () => controller.selectTab(task.workspaceTab!),
                  child: Text(
                    'Open ${switch (task.workspaceTab!) {
                      WorkspaceTab.browser => 'browser',
                      WorkspaceTab.benchmark => 'benchmark',
                      WorkspaceTab.tasks => 'tasks',
                      WorkspaceTab.settings => 'settings',
                      WorkspaceTab.eventLog => 'event log',
                    }}',
                  ),
                ),
              if (task.kind == BrowserTaskKind.tool)
                OutlinedButton(
                  onPressed: task.canCancel
                      ? () => controller.cancelToolTask(task)
                      : null,
                  child: const Text('Cancel'),
                ),
            ],
          ),
        ],
      ),
    );
  }

  String _formatBytes(int value) {
    if (value >= 1024 * 1024 * 1024) {
      return '${(value / (1024 * 1024 * 1024)).toStringAsFixed(1)} GiB';
    }
    if (value >= 1024 * 1024) {
      return '${(value / (1024 * 1024)).toStringAsFixed(1)} MiB';
    }
    if (value >= 1024) {
      return '${(value / 1024).toStringAsFixed(1)} KiB';
    }
    return '$value B';
  }

  String _formatDateTime(DateTime value) {
    final twoDigitMonth = value.month.toString().padLeft(2, '0');
    final twoDigitDay = value.day.toString().padLeft(2, '0');
    final twoDigitHour = value.hour.toString().padLeft(2, '0');
    final twoDigitMinute = value.minute.toString().padLeft(2, '0');
    return '${value.year}-$twoDigitMonth-$twoDigitDay $twoDigitHour:$twoDigitMinute';
  }
}
