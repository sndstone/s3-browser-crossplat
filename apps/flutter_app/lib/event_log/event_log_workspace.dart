import 'package:flutter/material.dart';

import '../controllers/app_controller.dart';
import '../logs/structured_log_list.dart';

class EventLogWorkspace extends StatelessWidget {
  const EventLogWorkspace({
    super.key,
    required this.controller,
  });

  final AppController controller;

  @override
  Widget build(BuildContext context) {
    final entries = controller.eventLog;
    final compact = MediaQuery.sizeOf(context).width < 700;

    return Padding(
      padding: const EdgeInsets.all(16),
      child: Card(
        clipBehavior: Clip.antiAlias,
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              compact
                  ? Wrap(
                      spacing: 12,
                      runSpacing: 12,
                      crossAxisAlignment: WrapCrossAlignment.center,
                      children: [
                        Text('Event Log',
                            style: Theme.of(context).textTheme.titleLarge),
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
                        OutlinedButton.icon(
                          onPressed: controller.clearEventLog,
                          icon: const Icon(Icons.clear_all),
                          label: const Text('Clear'),
                        ),
                      ],
                    )
                  : Row(
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
              const SizedBox(height: 16),
              Expanded(
                child: StructuredLogList(
                  entries: entries,
                  textScalePercent: controller.settings.logTextScalePercent,
                  emptyMessage: 'No events recorded yet.',
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
