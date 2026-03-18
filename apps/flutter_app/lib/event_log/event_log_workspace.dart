import 'package:flutter/material.dart';

import '../controllers/app_controller.dart';

class EventLogWorkspace extends StatelessWidget {
  const EventLogWorkspace({
    super.key,
    required this.controller,
  });

  final AppController controller;

  @override
  Widget build(BuildContext context) {
    final entries = controller.eventLog;
    final latestExportPath = controller.lastExportedEventLogPath?.trim();

    return Padding(
      padding: const EdgeInsets.all(16),
      child: Card(
        clipBehavior: Clip.antiAlias,
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Text('Event Log',
                      style: Theme.of(context).textTheme.titleLarge),
                  const Spacer(),
                  FilledButton.icon(
                    onPressed: controller.isBusy('export-event-log')
                        ? null
                        : controller.exportEventLog,
                    icon: const Icon(Icons.download_outlined),
                    label: Text(
                      controller.isBusy('export-event-log')
                          ? 'Exporting...'
                          : 'Export',
                    ),
                  ),
                  const SizedBox(width: 8),
                  OutlinedButton.icon(
                    onPressed: controller.clearEventLog,
                    icon: const Icon(Icons.clear_all),
                    label: const Text('Clear'),
                  ),
                ],
              ),
              const SizedBox(height: 8),
              const Text(
                'Every profile save/test, bucket refresh, object refresh, transfer action, and benchmark action is recorded here. When API logging or debug logging is enabled in Settings, raw engine request/response traces are included here as well.',
              ),
              if (latestExportPath != null && latestExportPath.isNotEmpty) ...[
                const SizedBox(height: 16),
                _exportLocationTile(context, latestExportPath),
              ],
              const SizedBox(height: 16),
              Expanded(
                child: entries.isEmpty
                    ? const Center(
                        child: Text('No events recorded yet.'),
                      )
                    : ListView.separated(
                        itemCount: entries.length,
                        separatorBuilder: (_, __) => const Divider(height: 1),
                        itemBuilder: (context, index) {
                          final entry = entries[index];
                          return ListTile(
                            contentPadding: EdgeInsets.zero,
                            dense: true,
                            title: Text('[${entry.level}] ${entry.category}'),
                            subtitle: Text(
                              '${_formatDateTime(entry.timestamp)}'
                              '${entry.profileId == null ? '' : '\nProfile: ${entry.profileId}'}'
                              '${entry.bucketName == null ? '' : '\nBucket: ${entry.bucketName}'}'
                              '${entry.objectKey == null ? '' : '\nObject: ${entry.objectKey}'}'
                              '\n${entry.message}',
                            ),
                            isThreeLine: true,
                          );
                        },
                      ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _exportLocationTile(BuildContext context, String path) {
    final theme = Theme.of(context);
    return InkWell(
      borderRadius: BorderRadius.circular(14),
      onTap: () => controller.openPath(path),
      child: Container(
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(14),
          border: Border.all(color: theme.colorScheme.outlineVariant),
          color: theme.colorScheme.surfaceContainerLowest,
        ),
        child: Row(
          children: [
            const Icon(Icons.description_outlined),
            const SizedBox(width: 10),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text('Latest export', style: theme.textTheme.labelLarge),
                  const SizedBox(height: 2),
                  Text(
                    path,
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: theme.textTheme.bodySmall?.copyWith(
                      color: theme.colorScheme.primary,
                    ),
                  ),
                ],
              ),
            ),
            IconButton(
              tooltip: 'Open log',
              onPressed: () => controller.openPath(path),
              icon: const Icon(Icons.open_in_new_outlined),
            ),
            IconButton(
              tooltip: 'Open file location',
              onPressed: () => controller.openPath(path, revealInFolder: true),
              icon: const Icon(Icons.folder_open_outlined),
            ),
          ],
        ),
      ),
    );
  }

  String _formatDateTime(DateTime value) {
    final twoDigitMonth = value.month.toString().padLeft(2, '0');
    final twoDigitDay = value.day.toString().padLeft(2, '0');
    final twoDigitHour = value.hour.toString().padLeft(2, '0');
    final twoDigitMinute = value.minute.toString().padLeft(2, '0');
    final twoDigitSecond = value.second.toString().padLeft(2, '0');
    return '${value.year}-$twoDigitMonth-$twoDigitDay $twoDigitHour:$twoDigitMinute:$twoDigitSecond';
  }
}
