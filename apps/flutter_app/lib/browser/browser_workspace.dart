import 'dart:io';

import 'package:desktop_drop/desktop_drop.dart';
import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';

import '../controllers/app_controller.dart';
import '../logs/structured_log_list.dart';
import '../models/domain_models.dart';

const _bucketActionBarKey = ValueKey('bucket-panel-actions');
const _bucketListKey = ValueKey('bucket-panel-scroll');
const _bucketProfileSummaryKey = ValueKey('bucket-profile-summary');

enum _MobileBrowserSection {
  buckets,
  objects,
  inspector,
}

class BrowserWorkspace extends StatefulWidget {
  const BrowserWorkspace({
    super.key,
    required this.controller,
    required this.compact,
  });

  final AppController controller;
  final bool compact;

  @override
  State<BrowserWorkspace> createState() => _BrowserWorkspaceState();
}

class _BrowserWorkspaceState extends State<BrowserWorkspace> {
  AppController get controller => widget.controller;
  double? _pendingInspectorSize;
  _MobileBrowserSection _mobileSection = _MobileBrowserSection.objects;

  AppSettings get _settings => controller.settings;

  double _resolveInspectorSize(
      BoxConstraints constraints, bool inspectorOnRight) {
    final rawSize =
        _pendingInspectorSize ?? _settings.browserInspectorSize.toDouble();
    if (inspectorOnRight) {
      return rawSize.clamp(280.0, constraints.maxWidth * 0.42);
    }
    return rawSize.clamp(240.0, constraints.maxHeight * 0.6);
  }

  void _updateInspectorSize(double nextSize) {
    setState(() {
      _pendingInspectorSize = nextSize;
    });
  }

  Future<void> _persistInspectorSize() async {
    final nextSize = _pendingInspectorSize?.round();
    if (nextSize == null || nextSize == _settings.browserInspectorSize) {
      return;
    }
    await controller.updateSettings(
      _settings.copyWith(browserInspectorSize: nextSize),
    );
  }

  Future<void> _pickFilesAndUpload() async {
    final picked = await FilePicker.platform.pickFiles(
      allowMultiple: true,
    );
    if (picked == null) {
      return;
    }
    final paths = picked.files.map((file) => file.path).whereType<String>().toList();
    if (paths.isEmpty) {
      return;
    }
    await controller.startSampleUpload(paths);
  }

  Widget _mobileBrowserShell(BuildContext context) {
    final hasProfile = controller.selectedProfile != null;
    final hasBucket = controller.selectedBucket != null;
    final effectiveSection = !hasProfile
        ? _MobileBrowserSection.buckets
        : (!hasBucket && _mobileSection != _MobileBrowserSection.buckets)
            ? _MobileBrowserSection.buckets
            : _mobileSection;
    final duration = controller.settings.enableAnimations
        ? const Duration(milliseconds: 260)
        : Duration.zero;

    return SingleChildScrollView(
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 20),
      child: Column(
        children: [
          SizedBox(
            width: double.infinity,
            child: SegmentedButton<_MobileBrowserSection>(
              segments: const [
                ButtonSegment(
                  value: _MobileBrowserSection.buckets,
                  icon: Icon(Icons.storage_outlined),
                  label: Text('Buckets'),
                ),
                ButtonSegment(
                  value: _MobileBrowserSection.objects,
                  icon: Icon(Icons.topic_outlined),
                  label: Text('Objects'),
                ),
                ButtonSegment(
                  value: _MobileBrowserSection.inspector,
                  icon: Icon(Icons.manage_search_outlined),
                  label: Text('Inspect'),
                ),
              ],
              selected: {effectiveSection},
              showSelectedIcon: false,
              onSelectionChanged: (selection) {
                setState(() {
                  _mobileSection = selection.first;
                });
              },
            ),
          ),
          const SizedBox(height: 14),
          AnimatedSwitcher(
            duration: duration,
            switchInCurve: Curves.easeOutCubic,
            switchOutCurve: Curves.easeInCubic,
            child: KeyedSubtree(
              key: ValueKey(effectiveSection),
              child: switch (effectiveSection) {
                _MobileBrowserSection.buckets =>
                  _bucketPanel(context, compact: true),
                _MobileBrowserSection.objects =>
                  _objectPanel(context, compact: true),
                _MobileBrowserSection.inspector =>
                  _inspectorPanel(context, compact: true),
              },
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final content = LayoutBuilder(
      builder: (context, constraints) {
        final width = constraints.maxWidth;
        if (width < 700) {
          return _mobileBrowserShell(context);
        }

        final inspectorOnRight =
            _settings.browserInspectorLayout == BrowserInspectorLayout.right &&
                width >= 1100;
        final inspectorSize =
            _resolveInspectorSize(constraints, inspectorOnRight);

        return Padding(
          padding: const EdgeInsets.all(16),
          child: Row(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              SizedBox(
                  width: 320, child: _bucketPanel(context, compact: false)),
              const SizedBox(width: 16),
              Expanded(
                child: inspectorOnRight
                    ? Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Expanded(
                              child: _objectPanel(context, compact: false)),
                          _resizeHandle(Axis.horizontal),
                          SizedBox(
                            width: inspectorSize,
                            child: _inspectorPanel(context, compact: false),
                          ),
                        ],
                      )
                    : Column(
                        children: [
                          Expanded(
                              child: _objectPanel(context, compact: false)),
                          _resizeHandle(Axis.vertical),
                          SizedBox(
                            height: inspectorSize,
                            child: _inspectorPanel(context, compact: false),
                          ),
                        ],
                      ),
              ),
            ],
          ),
        );
      },
    );

    if (Platform.isAndroid || MediaQuery.sizeOf(context).width < 700) {
      return content;
    }

    return DropTarget(
      onDragDone: (detail) async {
        final files = detail.files.map((file) => file.path).toList();
        await controller.startSampleUpload(files);
      },
      child: content,
    );
  }

  Future<void> _showCreatePrefixDialog(BuildContext context) async {
    final nameController = TextEditingController();

    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Create prefix'),
        content: SizedBox(
          width: 420,
          child: TextField(
            controller: nameController,
            autofocus: true,
            decoration: const InputDecoration(
              labelText: 'Prefix name',
              hintText: 'reports/2026',
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Cancel'),
          ),
          FilledButton(
            onPressed: () async {
              final value = nameController.text.trim();
              if (value.isEmpty) {
                return;
              }
              Navigator.of(context).pop();
              await controller.createFolderMarker(value);
            },
            child: const Text('Create'),
          ),
        ],
      ),
    );

    nameController.dispose();
  }

  Future<void> _showMobileObjectActions(BuildContext context) async {
    final hasBucket = controller.selectedBucket != null;
    final hasSelectedObject = controller.selectedObject != null;
    await showModalBottomSheet<void>(
      context: context,
      showDragHandle: true,
      builder: (context) => SafeArea(
        child: Padding(
          padding: const EdgeInsets.fromLTRB(20, 8, 20, 24),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                'Object actions',
                style: Theme.of(context).textTheme.titleLarge,
              ),
              const SizedBox(height: 8),
              Text(
                'Use the Android picker for uploads and keep object tools within thumb reach.',
                style: Theme.of(context).textTheme.bodyMedium,
              ),
              const SizedBox(height: 18),
              Wrap(
                spacing: 10,
                runSpacing: 10,
                children: [
                  OutlinedButton.icon(
                    onPressed: hasBucket
                        ? () {
                            Navigator.of(context).pop();
                            controller.refreshObjects();
                          }
                        : null,
                    icon: const Icon(Icons.refresh),
                    label: const Text('Refresh'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasBucket
                        ? () {
                            Navigator.of(context).pop();
                            _showCreatePrefixDialog(context);
                          }
                        : null,
                    icon: const Icon(Icons.create_new_folder_outlined),
                    label: const Text('Create prefix'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasSelectedObject
                        ? () {
                            Navigator.of(context).pop();
                            controller.deleteSelectedObject();
                          }
                        : null,
                    icon: const Icon(Icons.delete_outline),
                    label: const Text('Delete'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasBucket
                        ? () {
                            Navigator.of(context).pop();
                            controller.showAllObjectsNow();
                          }
                        : null,
                    icon: const Icon(Icons.unfold_more),
                    label: const Text('Show all'),
                  ),
                ],
              ),
              const SizedBox(height: 8),
              SwitchListTile(
                value: controller.flatView,
                onChanged: hasBucket
                    ? (value) {
                        Navigator.of(context).pop();
                        controller.toggleFlatView(value);
                      }
                    : null,
                contentPadding: EdgeInsets.zero,
                title: const Text('Flat view'),
                subtitle: const Text('Show objects as a single list.'),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _resizeHandle(Axis axis) {
    final isHorizontal = axis == Axis.horizontal;
    return MouseRegion(
      cursor: isHorizontal
          ? SystemMouseCursors.resizeLeftRight
          : SystemMouseCursors.resizeUpDown,
      child: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onPanUpdate: (details) {
          final current = _pendingInspectorSize?.toDouble() ??
              _settings.browserInspectorSize.toDouble();
          _updateInspectorSize(
            current + (isHorizontal ? -details.delta.dx : -details.delta.dy),
          );
        },
        onPanEnd: (_) => _persistInspectorSize(),
        child: SizedBox(
          width: isHorizontal ? 14 : double.infinity,
          height: isHorizontal ? double.infinity : 14,
          child: Center(
            child: Container(
              width: isHorizontal ? 4 : 56,
              height: isHorizontal ? 56 : 4,
              decoration: BoxDecoration(
                borderRadius: BorderRadius.circular(999),
                color: Theme.of(context).colorScheme.outlineVariant,
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _bucketPanel(BuildContext context, {required bool compact}) {
    return BrowserBucketPanel(
      controller: controller,
      compact: compact,
      onCreateBucket: () => _showCreateBucketDialog(context),
      onDeleteBucket: (bucketName, {force = false}) =>
          _confirmDeleteBucket(context, bucketName, force: force),
      onEditBucketLifecycle: (bucket) => _showJsonEditorDialog(
        context,
        title: 'Lifecycle JSON',
        initialValue: controller.adminState?.bucketName == bucket.name
            ? controller.adminState!.lifecycleJson
            : '{\n  "Rules": []\n}',
        onSave: controller.saveBucketLifecycle,
      ),
      onEditBucketPolicy: (bucket) => _showJsonEditorDialog(
        context,
        title: 'Policy JSON',
        initialValue: controller.adminState?.bucketName == bucket.name
            ? controller.adminState!.policyJson
            : '{}',
        onSave: controller.saveBucketPolicy,
      ),
      onEditBucketEncryption: (bucket) => _showJsonEditorDialog(
        context,
        title: 'Encryption JSON',
        initialValue: controller.adminState?.bucketName == bucket.name
            ? controller.adminState!.encryptionJson
            : '{}',
        onSave: controller.saveBucketEncryption,
      ),
      onEditBucketTags: (bucket) => _showTagEditorDialog(
        context,
        initialTags: controller.adminState?.bucketName == bucket.name
            ? controller.adminState!.tags
            : const <String, String>{},
      ),
      onToggleBucketVersioning: (bucket, enabled) async {
        if (controller.selectedBucket?.name != bucket.name) {
          await controller.setSelectedBucket(bucket);
        }
        await controller.setBucketVersioning(enabled);
      },
      onCopyBucket: (bucket) => _showCopyBucketDialog(context, bucket),
      inlineSpinnerBuilder: _inlineSpinner,
      inlineStatBuilder: _inlineStat,
    );
  }

  Widget _objectPanel(BuildContext context, {required bool compact}) {
    final phone = MediaQuery.sizeOf(context).width < 700;
    final availableWidth = MediaQuery.sizeOf(context).width - 64;
    final phonePanelHeight =
        (MediaQuery.sizeOf(context).height * 0.72).clamp(600.0, 860.0);
    final hasProfile = controller.selectedProfile != null;
    final hasBucket = controller.selectedBucket != null;
    final hasSelectedObject = controller.selectedObject != null;
    final objects = controller.pagedVisibleObjects;
    final filteredObjectCount = controller.visibleObjects.length;
    final currentPrefix = controller.currentPrefix;
    final isRefreshingObjects = controller.isBusy('refresh-objects');
    final isUploading = controller.isBusy('upload');
    final isDownloading = controller.isBusy('download');
    final isDeleting = controller.isBusy('delete-object');
    final isSelectingObject = controller.isBusy('select-object');

    final Widget listView = objects.isEmpty
        ? Center(
            child: Padding(
              padding: const EdgeInsets.all(24),
              child: Text(
                hasBucket
                    ? 'No objects were returned for this bucket and prefix.'
                    : 'Select a bucket to load objects.',
                textAlign: TextAlign.center,
              ),
            ),
          )
        : ListView.separated(
            shrinkWrap: false,
            primary: false,
            physics: const AlwaysScrollableScrollPhysics(),
            itemBuilder: (context, index) {
              final object = objects[index];
              return ListTile(
                selected: controller.selectedObject?.key == object.key,
                leading: Icon(
                    object.isFolder ? Icons.folder : Icons.insert_drive_file),
                title: Text(object.name),
                subtitle: Text(
                  '${controller.objectContentType(object)} • ${_formatBytes(object.size)} • ${_formatDateTime(object.modifiedAt)}',
                ),
                trailing: object.isFolder
                    ? const Icon(Icons.arrow_forward_ios, size: 14)
                    : Text('${object.metadataCount} meta'),
                onTap: () => controller.setSelectedObject(object),
              );
            },
            separatorBuilder: (_, __) => const Divider(height: 1),
            itemCount: objects.length,
          );

    final panel = Card(
      clipBehavior: Clip.antiAlias,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Objects', style: Theme.of(context).textTheme.titleLarge),
            const SizedBox(height: 6),
            if (!hasProfile)
              const Text(
                'Create and select an endpoint profile to browse objects.',
              )
            else if (!hasBucket)
              const Text(
                'Select a bucket to browse objects.',
              )
            else
              Wrap(
                spacing: 8,
                runSpacing: 8,
                crossAxisAlignment: WrapCrossAlignment.center,
                children: [
                  ActionChip(
                    avatar: const Icon(Icons.home_outlined, size: 16),
                    label: const Text('Root'),
                    onPressed: () => controller.refreshObjects(prefix: ''),
                  ),
                  if (currentPrefix.isNotEmpty)
                    ActionChip(
                      avatar:
                          const Icon(Icons.subdirectory_arrow_left, size: 16),
                      label: Text(currentPrefix),
                      onPressed: controller.navigateUp,
                    ),
                ],
              ),
            const SizedBox(height: 12),
            Wrap(
              spacing: 12,
              runSpacing: 12,
              children: [
                SizedBox(
                  width: phone ? availableWidth : 168,
                  child: DropdownButtonFormField<BrowserFilterMode>(
                    initialValue: controller.objectFilterMode,
                    decoration: const InputDecoration(
                      labelText: 'Filter mode',
                    ),
                    items: const [
                      DropdownMenuItem(
                        value: BrowserFilterMode.prefix,
                        child: Text('Prefix'),
                      ),
                      DropdownMenuItem(
                        value: BrowserFilterMode.text,
                        child: Text('Text'),
                      ),
                      DropdownMenuItem(
                        value: BrowserFilterMode.regex,
                        child: Text('Regex'),
                      ),
                    ],
                    onChanged: hasBucket
                        ? (value) {
                            if (value != null) {
                              controller.setObjectFilterMode(value);
                            }
                          }
                        : null,
                  ),
                ),
                SizedBox(
                  width: phone ? availableWidth : 240,
                  child: TextFormField(
                    key: ValueKey(
                      'object-filter-${controller.objectFilterMode.name}-${controller.objectFilterValue}',
                    ),
                    initialValue: controller.objectFilterValue,
                    enabled: hasBucket,
                    decoration: InputDecoration(
                      labelText: switch (controller.objectFilterMode) {
                        BrowserFilterMode.prefix => 'Object filter (prefix)',
                        BrowserFilterMode.text => 'Object filter (text)',
                        BrowserFilterMode.regex => 'Object filter (regex)',
                      },
                      prefixIcon: Icon(
                        switch (controller.objectFilterMode) {
                          BrowserFilterMode.prefix =>
                            Icons.folder_open_outlined,
                          BrowserFilterMode.text => Icons.search,
                          BrowserFilterMode.regex => Icons.code,
                        },
                      ),
                    ),
                    onFieldSubmitted: (value) async {
                      await controller.applyObjectFilter(value);
                    },
                  ),
                ),
                SizedBox(
                  width: phone ? availableWidth : 220,
                  child: DropdownButtonFormField<BrowserObjectSortField>(
                    initialValue: controller.objectSortField,
                    isExpanded: true,
                    decoration: const InputDecoration(
                      labelText: 'Sort objects',
                    ),
                    items: const [
                      DropdownMenuItem(
                        value: BrowserObjectSortField.lastModified,
                        child: Text('Last modified'),
                      ),
                      DropdownMenuItem(
                        value: BrowserObjectSortField.name,
                        child: Text('Name'),
                      ),
                      DropdownMenuItem(
                        value: BrowserObjectSortField.size,
                        child: Text('Object size'),
                      ),
                      DropdownMenuItem(
                        value: BrowserObjectSortField.contentType,
                        child: Text('Content type'),
                      ),
                    ],
                    onChanged: hasBucket
                        ? (value) {
                            if (value != null) {
                              controller.setObjectSortField(value);
                            }
                          }
                        : null,
                  ),
                ),
                if (!phone)
                  IconButton(
                    tooltip: controller.objectSortDescending
                        ? 'Sort descending'
                        : 'Sort ascending',
                    onPressed:
                        hasBucket ? controller.toggleObjectSortDirection : null,
                    icon: Icon(
                      controller.objectSortDescending
                          ? Icons.arrow_downward
                          : Icons.arrow_upward,
                    ),
                  ),
              ],
            ),
            const SizedBox(height: 12),
            if (phone)
              Row(
                children: [
                  Expanded(
                    child: FilledButton.icon(
                      onPressed: hasBucket && !isUploading
                          ? _pickFilesAndUpload
                          : null,
                      icon: isUploading
                          ? _inlineSpinner()
                          : const Icon(Icons.upload_file),
                      label: Text(isUploading ? 'Uploading...' : 'Upload'),
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: OutlinedButton.icon(
                      onPressed: hasSelectedObject && !isDownloading
                          ? controller.startSampleDownload
                          : null,
                      icon: isDownloading
                          ? _inlineSpinner()
                          : const Icon(Icons.download),
                      label:
                          Text(isDownloading ? 'Downloading...' : 'Download'),
                    ),
                  ),
                  const SizedBox(width: 10),
                  IconButton.filledTonal(
                    tooltip: 'More actions',
                    onPressed: hasBucket
                        ? () => _showMobileObjectActions(context)
                        : null,
                    icon: const Icon(Icons.tune),
                  ),
                ],
              )
            else
              Wrap(
                spacing: 12,
                runSpacing: 12,
                children: [
                  FilledButton.icon(
                    onPressed:
                        hasBucket && !isUploading ? _pickFilesAndUpload : null,
                    icon: isUploading
                        ? _inlineSpinner()
                        : const Icon(Icons.upload_file),
                    label: Text(isUploading ? 'Uploading...' : 'Upload'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasSelectedObject && !isDownloading
                        ? controller.startSampleDownload
                        : null,
                    icon: isDownloading
                        ? _inlineSpinner()
                        : const Icon(Icons.download),
                    label:
                        Text(isDownloading ? 'Downloading...' : 'Download'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasSelectedObject && !isDeleting
                        ? controller.deleteSelectedObject
                        : null,
                    icon: isDeleting
                        ? _inlineSpinner()
                        : const Icon(Icons.delete_outline),
                    label: Text(isDeleting ? 'Deleting...' : 'Delete'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasBucket
                        ? () => _showCreatePrefixDialog(context)
                        : null,
                    icon: const Icon(Icons.create_new_folder_outlined),
                    label: const Text('Create prefix'),
                  ),
                  OutlinedButton.icon(
                    onPressed: hasBucket ? controller.showAllObjectsNow : null,
                    icon: const Icon(Icons.unfold_more),
                    label: const Text('Show all'),
                  ),
                  FilterChip(
                    selected: controller.flatView,
                    onSelected: hasBucket ? controller.toggleFlatView : null,
                    avatar: const Icon(Icons.view_stream_outlined, size: 18),
                    label: const Text('Flat view'),
                  ),
                  IconButton(
                    tooltip: 'Refresh object list',
                    onPressed: hasBucket && !isRefreshingObjects
                        ? controller.refreshObjects
                        : null,
                    icon: isRefreshingObjects
                        ? _inlineSpinner()
                        : const Icon(Icons.refresh),
                  ),
                ],
              ),
            const SizedBox(height: 12),
            if (isRefreshingObjects || isSelectingObject)
              const Padding(
                padding: EdgeInsets.only(bottom: 12),
                child: LinearProgressIndicator(),
              ),
            if (hasBucket)
              Padding(
                padding: const EdgeInsets.only(bottom: 12),
                child: Wrap(
                  spacing: 12,
                  runSpacing: 12,
                  crossAxisAlignment: WrapCrossAlignment.center,
                  children: [
                    Text(
                      controller.showAllObjects
                          ? 'Showing all $filteredObjectCount object(s)'
                          : 'Showing ${controller.currentObjectPageStart}-${controller.currentObjectPageEnd} of $filteredObjectCount object(s)',
                    ),
                    if (!controller.showAllObjects &&
                        controller.objectPageCount > 1) ...[
                      SizedBox(
                        width: 180,
                        child: DropdownButtonFormField<int>(
                          initialValue: controller.objectPage
                              .clamp(1, controller.objectPageCount)
                              .toInt(),
                          decoration: const InputDecoration(labelText: 'Page'),
                          items: List<DropdownMenuItem<int>>.generate(
                            controller.objectPageCount,
                            (index) => DropdownMenuItem<int>(
                              value: index + 1,
                              child: Text(
                                'Page ${index + 1}',
                              ),
                            ),
                          ),
                          onChanged: (value) {
                            if (value != null) {
                              controller.setObjectPage(value);
                            }
                          },
                        ),
                      ),
                      OutlinedButton.icon(
                        onPressed: controller.objectPage > 1
                            ? controller.previousObjectPage
                            : null,
                        icon: const Icon(Icons.chevron_left),
                        label: const Text('Prev'),
                      ),
                      OutlinedButton.icon(
                        onPressed:
                            controller.objectPage < controller.objectPageCount
                                ? controller.nextObjectPage
                                : null,
                        icon: const Icon(Icons.chevron_right),
                        label: const Text('Next'),
                      ),
                    ],
                    if (controller.showAllObjects)
                      OutlinedButton.icon(
                        onPressed: () => controller.setShowAllObjects(false),
                        icon: const Icon(Icons.grid_view_outlined),
                        label: const Text('Use pages'),
                      ),
                    Text(
                      '${AppController.objectPageSize} per page',
                      style: Theme.of(context).textTheme.bodySmall,
                    ),
                  ],
                ),
              ),
            Container(
              decoration: BoxDecoration(
                color: Theme.of(context).colorScheme.surfaceContainerHighest,
                borderRadius: BorderRadius.circular(14),
              ),
              padding: const EdgeInsets.all(12),
              child: Row(
                children: [
                  const Icon(Icons.drag_indicator),
                  const SizedBox(width: 8),
                  Expanded(
                    child: Text(hasBucket
                        ? (Platform.isAndroid
                            ? 'Use the system picker or share sheet to add files on Android.'
                            : 'Drag and drop files here to upload them into the current bucket prefix.')
                        : 'Uploads are enabled after you select an endpoint profile and bucket.'),
                  ),
                ],
              ),
            ),
            const SizedBox(height: 12),
            if (phone)
              Expanded(child: listView)
            else if (compact)
              SizedBox(
                height: (MediaQuery.sizeOf(context).height * 0.42)
                    .clamp(280.0, 520.0),
                child: listView,
              )
            else
              Expanded(child: listView),
          ],
        ),
      ),
    );

    if (phone) {
      return SizedBox(
        height: phonePanelHeight,
        child: panel,
      );
    }

    return panel;
  }

  Widget _inspectorPanel(BuildContext context, {required bool compact}) {
    final phone = MediaQuery.sizeOf(context).width < 700;
    final tab = controller.inspectorTab;
    final theme = Theme.of(context);
    final panelBody = AnimatedSwitcher(
      duration: const Duration(milliseconds: 220),
      child: switch (tab) {
        BrowserInspectorTab.bucketAdmin => _bucketAdminView(context),
        BrowserInspectorTab.bucketInfo => _bucketInfoView(context),
        BrowserInspectorTab.objectDetails => _objectDetailsView(context),
        BrowserInspectorTab.versions => _versionsView(context),
        BrowserInspectorTab.presign => _presignView(context),
        BrowserInspectorTab.tools => _toolsView(context),
        BrowserInspectorTab.eventsAndDebug => _eventsAndDebugView(context),
      },
    );

    return Card(
      clipBehavior: Clip.antiAlias,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text('Inspector', style: Theme.of(context).textTheme.titleLarge),
            const SizedBox(height: 12),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: BrowserInspectorTab.values.map((entry) {
                final selected = entry == tab;
                final icon = switch (entry) {
                  BrowserInspectorTab.bucketAdmin =>
                    Icons.admin_panel_settings_outlined,
                  BrowserInspectorTab.bucketInfo => Icons.info_outline,
                  BrowserInspectorTab.objectDetails => Icons.article_outlined,
                  BrowserInspectorTab.versions => Icons.history,
                  BrowserInspectorTab.presign => Icons.link,
                  BrowserInspectorTab.tools => Icons.build_circle_outlined,
                  BrowserInspectorTab.eventsAndDebug =>
                    Icons.bug_report_outlined,
                };
                final label = switch (entry) {
                  BrowserInspectorTab.bucketAdmin => 'Bucket config',
                  BrowserInspectorTab.bucketInfo => 'Bucket info',
                  BrowserInspectorTab.objectDetails => 'Object',
                  BrowserInspectorTab.versions => 'Versions',
                  BrowserInspectorTab.presign => 'Presign',
                  BrowserInspectorTab.tools => 'Tools',
                  BrowserInspectorTab.eventsAndDebug => 'Events & Debug',
                };
                return ChoiceChip(
                  selected: selected,
                  showCheckmark: false,
                  avatar: Icon(
                    icon,
                    size: 18,
                    color: selected
                        ? theme.colorScheme.primary
                        : theme.colorScheme.onSurfaceVariant,
                  ),
                  backgroundColor: theme.colorScheme.secondaryContainer
                      .withValues(alpha: 0.72),
                  selectedColor:
                      theme.colorScheme.primaryContainer.withValues(alpha: 0.9),
                  side: BorderSide.none,
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(999),
                  ),
                  labelStyle: theme.textTheme.labelLarge?.copyWith(
                    color: selected
                        ? theme.colorScheme.onPrimaryContainer
                        : theme.colorScheme.onSurface,
                  ),
                  label: Text(label),
                  onSelected: (_) => controller.setInspectorTab(entry),
                );
              }).toList(),
            ),
            const SizedBox(height: 12),
            if (phone)
              panelBody
            else if (compact)
              SizedBox(
                height: (MediaQuery.sizeOf(context).height * 0.62)
                    .clamp(420.0, 920.0),
                child: panelBody,
              )
            else
              Expanded(child: panelBody),
          ],
        ),
      ),
    );
  }

  Widget _bucketAdminView(BuildContext context) {
    final admin = controller.adminState;
    if (admin == null) {
      return const Center(
          child: Text('Select a bucket to inspect configuration details.'));
    }

    return _adaptivePanelListView(
      context,
      key: const ValueKey('bucket-admin'),
      children: [
        Text(
          'Manage bucket actions from the bucket list context menu.',
          style: Theme.of(context).textTheme.bodyMedium,
        ),
        const SizedBox(height: 12),
        _inlineStat('Selected bucket', admin.bucketName),
        _inlineStat(
          'Action surface',
          'Right-click the bucket or use the overflow menu in the bucket list.',
        ),
        const Divider(height: 28),
        Text('Lifecycle JSON', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _jsonBlock(admin.lifecycleJson),
        const SizedBox(height: 12),
        Text('Policy JSON', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _jsonBlock(admin.policyJson),
        const SizedBox(height: 12),
        Text('CORS JSON', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _jsonBlock(admin.corsJson),
        const SizedBox(height: 12),
        Text('Encryption JSON', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _jsonBlock(admin.encryptionJson),
      ],
    );
  }

  Widget _bucketInfoView(BuildContext context) {
    final bucket = controller.selectedBucket;
    final admin = controller.adminState;
    if (bucket == null) {
      return const Center(
        child: Text('Select a bucket to inspect bucket details.'),
      );
    }

    return _adaptivePanelListView(
      context,
      key: const ValueKey('bucket-info'),
      children: [
        Text(bucket.name, style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 12),
        if (admin != null)
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              _pill('Versioning', admin.versioningEnabled),
              _pill('Object lock', admin.objectLockEnabled),
              _pill('Lifecycle', admin.lifecycleEnabled),
              _pill('Policy', admin.policyAttached),
              _pill('CORS', admin.corsEnabled),
              _pill('Encryption', admin.encryptionEnabled),
            ],
          ),
        if (admin != null) const SizedBox(height: 12),
        _inlineStat('Bucket name', bucket.name),
        _inlineStat(
            'Region', bucket.region.isEmpty ? 'Unknown' : bucket.region),
        _inlineStat(
          'Created',
          bucket.createdAt == null
              ? 'Unknown'
              : _formatDateTime(bucket.createdAt!),
        ),
        _inlineStat('Approx objects', '~${bucket.objectCountHint}'),
        _inlineStat(
          'Current prefix',
          controller.currentPrefix.isEmpty ? 'Root' : controller.currentPrefix,
        ),
        _inlineStat('Visible objects', '${controller.visibleObjects.length}'),
        if (admin != null) ...[
          _inlineStat('Versioning state', admin.versioningStatus),
          if (admin.objectLockEnabled)
            _inlineStat(
              'Object lock',
              admin.objectLockMode == null
                  ? 'Enabled'
                  : '${admin.objectLockMode} • ${admin.objectLockRetentionDays ?? 0} day retention',
            ),
          _inlineStat('Encryption', admin.encryptionSummary),
          _inlineStat('Bucket tags', '${admin.tags.length} tag(s)'),
          _inlineStat(
              'Lifecycle rules', '${admin.lifecycleRules.length} rule(s)'),
          _inlineStat(
            'Bucket policy',
            admin.policyAttached ? 'Attached' : 'Not attached',
          ),
          _inlineStat(
            'CORS',
            admin.corsEnabled ? 'Configured' : 'Not configured',
          ),
        ] else
          Padding(
            padding: const EdgeInsets.only(top: 8),
            child: Text(
              'Bucket configuration details are still loading.',
              style: Theme.of(context).textTheme.bodyMedium,
            ),
          ),
        const Divider(height: 28),
        Text('Bucket tags', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        if (admin == null)
          const Text('Loading tags...')
        else if (admin.tags.isEmpty)
          const Text('No bucket tags configured.')
        else
          ...admin.tags.entries.map(
            (entry) => ListTile(
              contentPadding: EdgeInsets.zero,
              dense: true,
              title: Text(entry.key),
              trailing: Text(entry.value),
            ),
          ),
        const Divider(height: 28),
        Text('Lifecycle rules', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        if (admin == null)
          const Text('Loading lifecycle rules...')
        else if (admin.lifecycleRules.isEmpty)
          const Text('No lifecycle rules configured.')
        else
          ...admin.lifecycleRules.map(
            (rule) => Card(
              child: Padding(
                padding: const EdgeInsets.all(12),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Row(
                      children: [
                        Text(
                          rule.id,
                          style: Theme.of(context).textTheme.titleSmall,
                        ),
                        const Spacer(),
                        Chip(
                          label: Text(rule.enabled ? 'Enabled' : 'Disabled'),
                        ),
                      ],
                    ),
                    const SizedBox(height: 8),
                    Text('Prefix: ${rule.prefix}'),
                    if (rule.expirationDays != null)
                      Text('Expiration: ${rule.expirationDays} days'),
                    if (rule.transitionStorageClass != null)
                      Text(
                        'Transition: ${rule.transitionStorageClass} after ${rule.transitionDays} days',
                      ),
                    if (rule.nonCurrentExpirationDays != null)
                      Text(
                        'Non-current expiration: ${rule.nonCurrentExpirationDays} days',
                      ),
                    if (rule.abortIncompleteMultipartUploadDays != null)
                      Text(
                        'Abort incomplete multipart uploads after ${rule.abortIncompleteMultipartUploadDays} days',
                      ),
                  ],
                ),
              ),
            ),
          ),
      ],
    );
  }

  Widget _objectDetailsView(BuildContext context) {
    final details = controller.selectedObjectDetails;
    final object = controller.selectedObject;
    if (details == null || object == null) {
      return const Center(
          child:
              Text('Select an object to inspect metadata, headers, and tags.'));
    }

    return _adaptivePanelListView(
      context,
      key: const ValueKey('object-details'),
      children: [
        _inlineStat('Key', object.key),
        _inlineStat('Storage class', object.storageClass),
        _inlineStat('Last modified', _formatDateTime(object.modifiedAt)),
        _inlineStat('Size', _formatBytes(object.size)),
        const Divider(height: 28),
        Text('Metadata', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        ...details.metadata.entries.map(
          (entry) => ListTile(
            contentPadding: EdgeInsets.zero,
            dense: true,
            title: Text(entry.key),
            trailing: Text(entry.value),
          ),
        ),
        const Divider(height: 28),
        Text('Headers', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        ...details.headers.entries.map(
          (entry) => ListTile(
            contentPadding: EdgeInsets.zero,
            dense: true,
            title: Text(entry.key),
            trailing: Text(entry.value),
          ),
        ),
        const Divider(height: 28),
        Text('Tags', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        ...details.tags.entries.map(
          (entry) => ListTile(
            contentPadding: EdgeInsets.zero,
            dense: true,
            title: Text(entry.key),
            trailing: Text(entry.value),
          ),
        ),
      ],
    );
  }

  Widget _versionsView(BuildContext context) {
    final options = controller.versionBrowserOptions;
    final versions = controller.visibleVersions;
    final hasSelectedObject = controller.selectedObject != null;

    return _adaptivePanelListView(
      context,
      key: const ValueKey('versions'),
      children: [
        Wrap(
          spacing: 8,
          runSpacing: 8,
          children: [
            FilledButton.icon(
              onPressed: () {
                controller.updateVersionBrowserOptions(
                  options.copyWith(
                    filterMode: BrowserFilterMode.prefix,
                    filterValue: '',
                  ),
                );
              },
              icon: const Icon(Icons.visibility_outlined),
              label: const Text('Show all versions'),
            ),
            OutlinedButton.icon(
              onPressed: controller.refreshObjects,
              icon: const Icon(Icons.refresh),
              label: const Text('Refresh versions'),
            ),
            OutlinedButton.icon(
              onPressed:
                  hasSelectedObject ? controller.startSampleDownload : null,
              icon: const Icon(Icons.download),
              label: const Text('Download selected'),
            ),
            OutlinedButton.icon(
              onPressed:
                  hasSelectedObject ? controller.deleteSelectedObject : null,
              icon: const Icon(Icons.delete_outline),
              label: const Text('Delete selected'),
            ),
          ],
        ),
        const SizedBox(height: 12),
        if (!hasSelectedObject)
          const Padding(
            padding: EdgeInsets.only(bottom: 12),
            child:
                Text('Showing all versioned objects in the selected bucket.'),
          ),
        Row(
          children: [
            Expanded(
              flex: 2,
              child: DropdownButtonFormField<BrowserFilterMode>(
                initialValue: options.filterMode,
                decoration: const InputDecoration(labelText: 'Filter mode'),
                items: const [
                  DropdownMenuItem(
                    value: BrowserFilterMode.prefix,
                    child: Text('Prefix'),
                  ),
                  DropdownMenuItem(
                    value: BrowserFilterMode.text,
                    child: Text('Text'),
                  ),
                  DropdownMenuItem(
                    value: BrowserFilterMode.regex,
                    child: Text('Regex'),
                  ),
                ],
                onChanged: (value) {
                  if (value != null) {
                    controller.updateVersionBrowserOptions(
                      options.copyWith(filterMode: value),
                    );
                  }
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              flex: 3,
              child: TextFormField(
                key: ValueKey(
                  'version-filter-${options.filterMode.name}-${options.filterValue}',
                ),
                initialValue: options.filterValue,
                decoration: InputDecoration(
                  labelText: switch (options.filterMode) {
                    BrowserFilterMode.prefix => 'Version filter (prefix)',
                    BrowserFilterMode.text => 'Version filter (text)',
                    BrowserFilterMode.regex => 'Version filter (regex)',
                  },
                  prefixIcon: const Icon(Icons.filter_alt_outlined),
                ),
                onFieldSubmitted: (value) {
                  controller.updateVersionBrowserOptions(
                    options.copyWith(filterValue: value),
                  );
                },
              ),
            ),
          ],
        ),
        SwitchListTile(
          value: options.showVersions,
          onChanged: (value) {
            controller.updateVersionBrowserOptions(
              options.copyWith(showVersions: value),
            );
          },
          title: const Text('Show versions'),
        ),
        SwitchListTile(
          value: options.showDeleteMarkers,
          onChanged: (value) {
            controller.updateVersionBrowserOptions(
              options.copyWith(showDeleteMarkers: value),
            );
          },
          title: const Text('Show delete markers'),
        ),
        ListTile(
          contentPadding: EdgeInsets.zero,
          title: Text('Displayed entries: ${controller.displayedVersionCount}'),
          subtitle: Text(
            'Delete markers: ${controller.visibleDeleteMarkerCount}',
          ),
        ),
        const Divider(height: 20),
        ...versions.map(
          (version) => ListTile(
            contentPadding: EdgeInsets.zero,
            dense: true,
            title: Text(version.versionId),
            subtitle: Text(
              '${version.key}\n${version.storageClass} • ${_formatBytes(version.size)} • ${_formatDateTime(version.modifiedAt)}',
            ),
            isThreeLine: true,
            trailing: Text(
              version.deleteMarker
                  ? 'Delete marker'
                  : (version.latest ? 'Latest' : 'Prior'),
            ),
          ),
        ),
      ],
    );
  }

  Widget _presignView(BuildContext context) {
    final bundle = controller.selectedObjectDetails?.presignedUrl;
    return _adaptivePanelListView(
      context,
      key: const ValueKey('presign'),
      children: [
        _numberField(
          label: 'Expiration (minutes)',
          initialValue: controller.settings.defaultPresignMinutes,
          onSubmitted: (value) {
            controller.updateSettings(
              controller.settings.copyWith(defaultPresignMinutes: value),
            );
          },
        ),
        const SizedBox(height: 12),
        FilledButton.icon(
          onPressed: controller.generateSelectedPresignedUrl,
          icon: const Icon(Icons.link),
          label: const Text('Generate presigned URL'),
        ),
        const SizedBox(height: 16),
        if (bundle == null)
          const Text(
              'Generate a URL for the selected object to show the curl helper and expiration details.')
        else ...[
          _inlineStat('Expires', '${bundle.expirationMinutes} minutes'),
          const SizedBox(height: 8),
          SelectableText(bundle.url),
          const SizedBox(height: 16),
          Text('curl helper', style: Theme.of(context).textTheme.titleMedium),
          const SizedBox(height: 8),
          _jsonBlock(bundle.curlCommand),
        ],
      ],
    );
  }

  Widget _toolsView(BuildContext context) {
    final testData = controller.testDataConfig;
    final deleteAll = controller.deleteAllConfig;

    return _adaptivePanelListView(
      context,
      key: const ValueKey('tools'),
      children: [
        Text('Put test data', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _textField(
          label: 'Bucket',
          initialValue: testData.bucketName,
          onSubmitted: (value) {
            controller
                .updateTestDataConfig(testData.copyWith(bucketName: value));
          },
        ),
        const SizedBox(height: 8),
        _textField(
          label: 'Prefix',
          initialValue: testData.prefix,
          onSubmitted: (value) {
            controller.updateTestDataConfig(testData.copyWith(prefix: value));
          },
        ),
        const SizedBox(height: 8),
        _numberField(
          label: 'Object size (bytes)',
          initialValue: testData.objectSizeBytes,
          onSubmitted: (value) {
            controller.updateTestDataConfig(
              testData.copyWith(objectSizeBytes: value),
            );
          },
        ),
        const SizedBox(height: 8),
        Row(
          children: [
            Expanded(
              child: _numberField(
                label: 'Objects',
                initialValue: testData.objectCount,
                onSubmitted: (value) {
                  controller.updateTestDataConfig(
                      testData.copyWith(objectCount: value));
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _numberField(
                label: 'Versions',
                initialValue: testData.versions,
                onSubmitted: (value) {
                  controller
                      .updateTestDataConfig(testData.copyWith(versions: value));
                },
              ),
            ),
          ],
        ),
        const SizedBox(height: 8),
        _numberField(
          label: 'Threads',
          initialValue: testData.threads,
          onSubmitted: (value) {
            controller.updateTestDataConfig(testData.copyWith(threads: value));
          },
        ),
        FilledButton.icon(
          onPressed: controller.runPutTestDataTool,
          icon: const Icon(Icons.data_object),
          label: const Text('Run put-testdata.py'),
        ),
        ListTile(
          contentPadding: EdgeInsets.zero,
          title: Text(controller.putTestDataState.label),
          subtitle: Text(controller.putTestDataState.lastStatus),
        ),
        const Divider(height: 28),
        Text('Delete all', style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        _textField(
          label: 'Bucket',
          initialValue: deleteAll.bucketName,
          onSubmitted: (value) {
            controller
                .updateDeleteAllConfig(deleteAll.copyWith(bucketName: value));
          },
        ),
        const SizedBox(height: 8),
        Row(
          children: [
            Expanded(
              child: _numberField(
                label: 'Batch size',
                initialValue: deleteAll.batchSize,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                      deleteAll.copyWith(batchSize: value));
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _numberField(
                label: 'Workers',
                initialValue: deleteAll.maxWorkers,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                      deleteAll.copyWith(maxWorkers: value));
                },
              ),
            ),
          ],
        ),
        const SizedBox(height: 8),
        Row(
          children: [
            Expanded(
              child: _numberField(
                label: 'Connections',
                initialValue: deleteAll.maxConnections,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                    deleteAll.copyWith(maxConnections: value),
                  );
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _numberField(
                label: 'Pipeline size',
                initialValue: deleteAll.pipelineSize,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                      deleteAll.copyWith(pipelineSize: value));
                },
              ),
            ),
          ],
        ),
        const SizedBox(height: 8),
        Row(
          children: [
            Expanded(
              child: _numberField(
                label: 'List max keys',
                initialValue: deleteAll.listMaxKeys,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                      deleteAll.copyWith(listMaxKeys: value));
                },
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: _numberField(
                label: 'Delete delay (ms)',
                initialValue: deleteAll.deletionDelayMs,
                onSubmitted: (value) {
                  controller.updateDeleteAllConfig(
                    deleteAll.copyWith(deletionDelayMs: value),
                  );
                },
              ),
            ),
          ],
        ),
        SwitchListTile(
          value: deleteAll.immediateDeletion,
          onChanged: (value) {
            controller.updateDeleteAllConfig(
              deleteAll.copyWith(immediateDeletion: value),
            );
          },
          title: const Text('Immediate deletion'),
        ),
        FilledButton.icon(
          onPressed: controller.runDeleteAllTool,
          icon: const Icon(Icons.delete_sweep_outlined),
          label: const Text('Run delete-all.py'),
        ),
        ListTile(
          contentPadding: EdgeInsets.zero,
          title: Text(controller.deleteAllState.label),
          subtitle: Text(controller.deleteAllState.lastStatus),
        ),
      ],
    );
  }

  Widget _eventsAndDebugView(BuildContext context) {
    final details = controller.selectedObjectDetails;
    final scopedEvents = controller.bucketScopedEvents.where((entry) {
      if (details == null) {
        return true;
      }
      return entry.objectKey == null || entry.objectKey == details.key;
    }).toList();
    final debugEvents = details?.debugEvents ?? const <DiagnosticEvent>[];

    return _adaptivePanelListView(
      context,
      key: const ValueKey('events-and-debug'),
      children: [
        Wrap(
          spacing: 8,
          runSpacing: 8,
          children: [
            FilledButton.icon(
              onPressed: controller.isBusy('export-diagnostics')
                  ? null
                  : controller.exportDiagnostics,
              icon: const Icon(Icons.download_for_offline_outlined),
              label: Text(
                controller.isBusy('export-diagnostics')
                    ? 'Exporting...'
                    : 'Export debug log',
              ),
            ),
            OutlinedButton.icon(
              onPressed: controller.clearDiagnostics,
              icon: const Icon(Icons.clear_all),
              label: const Text('Clear object logs'),
            ),
          ],
        ),
        const SizedBox(height: 16),
        Text('Trace log',
            style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        StructuredLogList(
          entries: scopedEvents,
          textScalePercent: controller.settings.logTextScalePercent,
          emptyMessage: 'No bucket-scoped trace events recorded yet.',
          embedded: true,
        ),
        const Divider(height: 28),
        Text('Object debug events',
            style: Theme.of(context).textTheme.titleMedium),
        const SizedBox(height: 8),
        if (debugEvents.isEmpty)
          const Text('No object-specific debug events recorded.')
        else
          ...debugEvents.map(
            (event) => ListTile(
              contentPadding: EdgeInsets.zero,
              dense: true,
              title: Text('[${event.level}] ${event.message}'),
              subtitle: Text(_formatDateTime(event.timestamp)),
            ),
          ),
        if ((details?.debugLogExcerpt ?? const <String>[]).isNotEmpty) ...[
          const Divider(height: 28),
          Text('Debug excerpt', style: Theme.of(context).textTheme.titleMedium),
          const SizedBox(height: 8),
          _jsonBlock((details?.debugLogExcerpt ?? const <String>[]).join('\n')),
        ],
      ],
    );
  }

  Widget _adaptivePanelListView(
    BuildContext context, {
    required Key key,
    required List<Widget> children,
  }) {
    final phone = MediaQuery.sizeOf(context).width < 700;
    return ListView(
      key: key,
      shrinkWrap: phone,
      primary: false,
      physics: phone
          ? const NeverScrollableScrollPhysics()
          : const AlwaysScrollableScrollPhysics(),
      children: children,
    );
  }

  Widget _inlineStat(String label, String value) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.only(bottom: 6),
      child: RichText(
        text: TextSpan(
          style: theme.textTheme.bodyMedium?.copyWith(
            height: 1.35,
            color: theme.colorScheme.onSurface,
          ),
          children: [
            TextSpan(
              text: '$label: ',
              style: TextStyle(
                fontWeight: FontWeight.w700,
                color: theme.colorScheme.onSurfaceVariant,
              ),
            ),
            TextSpan(text: value),
          ],
        ),
      ),
    );
  }

  Widget _pill(String label, bool enabled) {
    return Chip(
      avatar: Icon(
        enabled ? Icons.check_circle : Icons.block,
        size: 16,
      ),
      label: Text(label),
    );
  }

  Widget _jsonBlock(String value) {
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(14),
        color: const Color(0x11000000),
      ),
      child: SelectableText(value),
    );
  }

  Widget _inlineSpinner() {
    return const SizedBox(
      width: 18,
      height: 18,
      child: CircularProgressIndicator(strokeWidth: 2),
    );
  }

  Widget _textField({
    required String label,
    required String initialValue,
    required ValueChanged<String> onSubmitted,
  }) {
    return TextFormField(
      initialValue: initialValue,
      decoration: InputDecoration(labelText: label),
      onFieldSubmitted: onSubmitted,
    );
  }

  Widget _numberField({
    required String label,
    required int initialValue,
    required ValueChanged<int> onSubmitted,
  }) {
    return TextFormField(
      initialValue: '$initialValue',
      keyboardType: TextInputType.number,
      decoration: InputDecoration(labelText: label),
      onFieldSubmitted: (value) {
        final parsed = int.tryParse(value);
        if (parsed != null) {
          onSubmitted(parsed);
        }
      },
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

  Future<void> _showCreateBucketDialog(BuildContext context) async {
    final nameController = TextEditingController();
    var enableVersioning = false;
    var enableObjectLock = false;

    await showDialog<void>(
      context: context,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setState) {
            return AlertDialog(
              title: const Text('Create bucket'),
              content: SizedBox(
                width: 420,
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    TextField(
                      controller: nameController,
                      decoration: const InputDecoration(
                        labelText: 'Bucket name',
                      ),
                    ),
                    const SizedBox(height: 12),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      value: enableVersioning,
                      onChanged: (value) =>
                          setState(() => enableVersioning = value),
                      title: const Text('Enable versioning'),
                    ),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      value: enableObjectLock,
                      onChanged: (value) =>
                          setState(() => enableObjectLock = value),
                      title: const Text('Enable object lock'),
                    ),
                    const Align(
                      alignment: Alignment.centerLeft,
                      child: Text(
                        'Object lock must be enabled when the bucket is created.',
                      ),
                    ),
                  ],
                ),
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.of(context).pop(),
                  child: const Text('Cancel'),
                ),
                FilledButton(
                  onPressed: () async {
                    final bucketName = nameController.text.trim();
                    if (bucketName.isEmpty) {
                      return;
                    }
                    Navigator.of(context).pop();
                    await controller.createBucket(
                      bucketName: bucketName,
                      enableVersioning: enableVersioning,
                      enableObjectLock: enableObjectLock,
                    );
                  },
                  child: const Text('Create'),
                ),
              ],
            );
          },
        );
      },
    );

    nameController.dispose();
  }

  Future<void> _confirmDeleteBucket(
    BuildContext context,
    String bucketName, {
    bool force = false,
  }) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: Text(force ? 'Force delete bucket' : 'Delete bucket'),
        content: Text(
          force
              ? 'Delete every object found in "$bucketName" with the delete-all tool, then delete the bucket itself?'
              : 'Delete "$bucketName"? If the bucket is not empty, use Force delete instead.',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(false),
            child: const Text('Cancel'),
          ),
          FilledButton(
            onPressed: () => Navigator.of(context).pop(true),
            child: Text(force ? 'Force delete' : 'Delete'),
          ),
        ],
      ),
    );
    if (confirmed == true) {
      await controller.deleteBucketByName(bucketName, force: force);
    }
  }

  Future<void> _showCopyBucketDialog(
    BuildContext context,
    BucketSummary sourceBucket,
  ) async {
    final destinationController = TextEditingController();
    var createDestination = false;
    final initialDestinations = controller.buckets
        .where((bucket) => bucket.name != sourceBucket.name)
        .map((bucket) => bucket.name)
        .toList();
    String? selectedDestination =
        initialDestinations.isEmpty ? null : initialDestinations.first;
    destinationController.text = selectedDestination ?? '';

    await showDialog<void>(
      context: context,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setState) {
            final destinations = controller.buckets
                .where((bucket) => bucket.name != sourceBucket.name)
                .map((bucket) => bucket.name)
                .toList();
            return AlertDialog(
              title: Text('Copy ${sourceBucket.name}'),
              content: SizedBox(
                width: 420,
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    DropdownButtonFormField<String>(
                      initialValue: destinations.contains(selectedDestination)
                          ? selectedDestination
                          : null,
                      decoration: const InputDecoration(
                        labelText: 'Destination bucket',
                      ),
                      items: destinations
                          .map(
                            (bucketName) => DropdownMenuItem(
                              value: bucketName,
                              child: Text(bucketName),
                            ),
                          )
                          .toList(),
                      onChanged: (value) {
                        setState(() {
                          selectedDestination = value;
                          destinationController.text = value ?? '';
                        });
                      },
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: destinationController,
                      decoration: const InputDecoration(
                        labelText: 'Or enter a new destination bucket',
                      ),
                      onChanged: (value) {
                        setState(() {
                          selectedDestination =
                              value.trim().isEmpty ? null : value;
                        });
                      },
                    ),
                    const SizedBox(height: 12),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      value: createDestination,
                      onChanged: (value) =>
                          setState(() => createDestination = value),
                      title: const Text('Create destination if missing'),
                    ),
                    const Align(
                      alignment: Alignment.centerLeft,
                      child: Text(
                        'Copies bucket contents only. Lifecycle, policy, encryption, and tagging stay independent.',
                      ),
                    ),
                  ],
                ),
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.of(context).pop(),
                  child: const Text('Cancel'),
                ),
                FilledButton(
                  onPressed: () async {
                    final destinationBucketName =
                        destinationController.text.trim();
                    if (destinationBucketName.isEmpty) {
                      return;
                    }
                    Navigator.of(context).pop();
                    await controller.copyBucketContents(
                      sourceBucketName: sourceBucket.name,
                      destinationBucketName: destinationBucketName,
                      createDestinationIfMissing: createDestination,
                    );
                  },
                  child: const Text('Copy bucket'),
                ),
              ],
            );
          },
        );
      },
    );

    destinationController.dispose();
  }

  Future<void> _showJsonEditorDialog(
    BuildContext context, {
    required String title,
    required String initialValue,
    required Future<void> Function(String value) onSave,
  }) async {
    final controllerText = TextEditingController(text: initialValue);
    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: Text(title),
        content: SizedBox(
          width: 560,
          child: TextField(
            controller: controllerText,
            minLines: 12,
            maxLines: 20,
            decoration: const InputDecoration(
              border: OutlineInputBorder(),
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Cancel'),
          ),
          FilledButton(
            onPressed: () async {
              Navigator.of(context).pop();
              await onSave(controllerText.text.trim());
            },
            child: const Text('Save'),
          ),
        ],
      ),
    );
    controllerText.dispose();
  }

  Future<void> _showTagEditorDialog(
    BuildContext context, {
    required Map<String, String> initialTags,
  }) async {
    final controllerText = TextEditingController(
      text: initialTags.entries
          .map((entry) => '${entry.key}=${entry.value}')
          .join('\n'),
    );
    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Bucket tags'),
        content: SizedBox(
          width: 460,
          child: TextField(
            controller: controllerText,
            minLines: 8,
            maxLines: 16,
            decoration: const InputDecoration(
              labelText: 'One key=value pair per line',
              border: OutlineInputBorder(),
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Cancel'),
          ),
          FilledButton(
            onPressed: () async {
              final tags = <String, String>{};
              for (final line in controllerText.text.split('\n')) {
                final trimmed = line.trim();
                if (trimmed.isEmpty) {
                  continue;
                }
                final separator = trimmed.indexOf('=');
                if (separator <= 0) {
                  continue;
                }
                tags[trimmed.substring(0, separator).trim()] =
                    trimmed.substring(separator + 1).trim();
              }
              Navigator.of(context).pop();
              await controller.saveBucketTags(tags);
            },
            child: const Text('Save'),
          ),
        ],
      ),
    );
    controllerText.dispose();
  }
}

class BrowserBucketPanel extends StatefulWidget {
  const BrowserBucketPanel({
    super.key,
    required this.controller,
    required this.compact,
    required this.onCreateBucket,
    required this.onDeleteBucket,
    required this.onEditBucketLifecycle,
    required this.onEditBucketPolicy,
    required this.onEditBucketEncryption,
    required this.onEditBucketTags,
    required this.onToggleBucketVersioning,
    required this.onCopyBucket,
    required this.inlineSpinnerBuilder,
    required this.inlineStatBuilder,
  });

  final AppController controller;
  final bool compact;
  final VoidCallback onCreateBucket;
  final Future<void> Function(String bucketName, {bool force}) onDeleteBucket;
  final Future<void> Function(BucketSummary bucket) onEditBucketLifecycle;
  final Future<void> Function(BucketSummary bucket) onEditBucketPolicy;
  final Future<void> Function(BucketSummary bucket) onEditBucketEncryption;
  final Future<void> Function(BucketSummary bucket) onEditBucketTags;
  final Future<void> Function(BucketSummary bucket, bool enabled)
      onToggleBucketVersioning;
  final Future<void> Function(BucketSummary bucket) onCopyBucket;
  final Widget Function() inlineSpinnerBuilder;
  final Widget Function(String label, String value) inlineStatBuilder;

  @override
  State<BrowserBucketPanel> createState() => _BrowserBucketPanelState();
}

class _BrowserBucketPanelState extends State<BrowserBucketPanel> {
  final ScrollController _bucketScrollController = ScrollController();

  Future<void> _showBucketMenu(
    BuildContext context,
    BucketSummary bucket,
    Offset position,
  ) async {
    final selected = await showMenu<String>(
      context: context,
      position: RelativeRect.fromLTRB(
        position.dx,
        position.dy,
        position.dx,
        position.dy,
      ),
      items: [
        const PopupMenuItem(
          value: 'open',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.folder_open_outlined),
            title: Text('Open bucket'),
          ),
        ),
        PopupMenuItem(
          value: bucket.versioningEnabled
              ? 'suspend-versioning'
              : 'enable-versioning',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(
              bucket.versioningEnabled
                  ? Icons.pause_circle_outline
                  : Icons.history_toggle_off_rounded,
            ),
            title: Text(
              bucket.versioningEnabled
                  ? 'Suspend versioning'
                  : 'Enable versioning',
            ),
          ),
        ),
        const PopupMenuDivider(),
        const PopupMenuItem(
          value: 'lifecycle',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.schedule_outlined),
            title: Text('Lifecycle policy'),
          ),
        ),
        const PopupMenuItem(
          value: 'policy',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.policy_outlined),
            title: Text('Bucket policy'),
          ),
        ),
        const PopupMenuItem(
          value: 'encryption',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.lock_outline),
            title: Text('Bucket encryption'),
          ),
        ),
        const PopupMenuItem(
          value: 'tags',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.sell_outlined),
            title: Text('Bucket tagging'),
          ),
        ),
        const PopupMenuItem(
          value: 'copy',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.copy_all_outlined),
            title: Text('Copy bucket'),
          ),
        ),
        const PopupMenuDivider(),
        const PopupMenuItem(
          value: 'delete',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.delete_outline),
            title: Text('Delete bucket'),
          ),
        ),
        const PopupMenuItem(
          value: 'force-delete',
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(Icons.delete_forever_outlined),
            title: Text('Force delete bucket'),
          ),
        ),
      ],
    );
    if (!mounted || selected == null) {
      return;
    }
    if (widget.controller.selectedBucket?.name != bucket.name) {
      await widget.controller.setSelectedBucket(bucket);
    }
    switch (selected) {
      case 'open':
        await widget.controller.setSelectedBucket(bucket);
        return;
      case 'enable-versioning':
        await widget.onToggleBucketVersioning(bucket, true);
        return;
      case 'suspend-versioning':
        await widget.onToggleBucketVersioning(bucket, false);
        return;
      case 'lifecycle':
        await widget.onEditBucketLifecycle(bucket);
        return;
      case 'policy':
        await widget.onEditBucketPolicy(bucket);
        return;
      case 'encryption':
        await widget.onEditBucketEncryption(bucket);
        return;
      case 'tags':
        await widget.onEditBucketTags(bucket);
        return;
      case 'copy':
        await widget.onCopyBucket(bucket);
        return;
      case 'delete':
        await widget.onDeleteBucket(bucket.name);
        return;
      case 'force-delete':
        await widget.onDeleteBucket(bucket.name, force: true);
        return;
      default:
        return;
    }
  }

  @override
  void dispose() {
    _bucketScrollController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final controller = widget.controller;
    final profile = controller.selectedProfile;
    final buckets = controller.buckets;
    final hasProfile = profile != null;
    final isRefreshing = controller.isBusy('refresh-buckets');
    final isCreatingBucket = controller.isBusy('create-bucket');
    final isDeletingBucket = controller.isBusy('delete-bucket');

    final bucketListContent = ListView(
      key: _bucketListKey,
      controller: _bucketScrollController,
      padding: EdgeInsets.zero,
      primary: false,
      shrinkWrap: widget.compact,
      physics: widget.compact
          ? const NeverScrollableScrollPhysics()
          : const AlwaysScrollableScrollPhysics(),
      children: [
        if (buckets.isEmpty && hasProfile)
          const Padding(
            padding: EdgeInsets.only(bottom: 12),
            child: Text('No buckets loaded yet for this endpoint.'),
          )
        else
          ...buckets.map(
            (bucket) => Builder(
              builder: (context) {
                return InkWell(
                  onSecondaryTapDown: (details) => _showBucketMenu(
                    context,
                    bucket,
                    details.globalPosition,
                  ),
                  child: ListTile(
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(14),
                    ),
                    selected: controller.selectedBucket?.name == bucket.name,
                    title: Text(bucket.name),
                    subtitle: Text(
                      '${bucket.region} • ~${bucket.objectCountHint} objects',
                    ),
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        if (bucket.versioningEnabled)
                          const Padding(
                            padding: EdgeInsets.only(right: 4),
                            child: Icon(Icons.history_toggle_off_rounded),
                          ),
                        IconButton(
                          tooltip: 'Bucket actions',
                          onPressed: () async {
                            final box =
                                context.findRenderObject() as RenderBox?;
                            if (box == null) {
                              return;
                            }
                            await _showBucketMenu(
                              context,
                              bucket,
                              box.localToGlobal(
                                Offset(
                                    box.size.width - 24, box.size.height / 2),
                              ),
                            );
                          },
                          icon: const Icon(Icons.more_horiz),
                        ),
                      ],
                    ),
                    onTap: () => controller.setSelectedBucket(bucket),
                  ),
                );
              },
            ),
          ),
      ],
    );

    final bucketListViewport = Scrollbar(
      controller: _bucketScrollController,
      thumbVisibility: true,
      interactive: true,
      child: bucketListContent,
    );

    return Card(
      clipBehavior: Clip.antiAlias,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          mainAxisSize: widget.compact ? MainAxisSize.min : MainAxisSize.max,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Text('Buckets', style: Theme.of(context).textTheme.titleLarge),
                const Spacer(),
                IconButton(
                  tooltip: 'Refresh buckets',
                  onPressed: hasProfile && !isRefreshing
                      ? controller.refreshBuckets
                      : null,
                  icon: isRefreshing
                      ? widget.inlineSpinnerBuilder()
                      : const Icon(Icons.refresh),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Wrap(
              key: _bucketActionBarKey,
              spacing: 12,
              runSpacing: 12,
              children: [
                FilledButton.icon(
                  onPressed: hasProfile && !isCreatingBucket
                      ? widget.onCreateBucket
                      : null,
                  icon: isCreatingBucket
                      ? widget.inlineSpinnerBuilder()
                      : const Icon(Icons.add_circle_outline),
                  label:
                      Text(isCreatingBucket ? 'Creating...' : 'Create bucket'),
                ),
                if (controller.selectedBucket != null)
                  TextButton.icon(
                    onPressed: hasProfile && !isDeletingBucket
                        ? () => widget.onDeleteBucket(
                              controller.selectedBucket!.name,
                            )
                        : null,
                    icon: isDeletingBucket
                        ? widget.inlineSpinnerBuilder()
                        : const Icon(Icons.delete_forever_outlined),
                    label: const Text('Delete selected'),
                  ),
              ],
            ),
            if (!hasProfile)
              const Padding(
                padding: EdgeInsets.only(top: 8),
                child: Text(
                  'No endpoint profile is selected. Create one in Settings, save it, then come back here to list buckets.',
                ),
              ),
            const SizedBox(height: 12),
            if (MediaQuery.sizeOf(context).width < 700)
              bucketListViewport
            else if (widget.compact)
              SizedBox(
                height: (MediaQuery.sizeOf(context).height * 0.34)
                    .clamp(240.0, 360.0),
                child: bucketListViewport,
              )
            else
              Expanded(
                child: bucketListViewport,
              ),
            if (profile != null) ...[
              const SizedBox(height: 12),
              _profileSummary(context, profile),
            ],
          ],
        ),
      ),
    );
  }

  Widget _profileSummary(BuildContext context, EndpointProfile profile) {
    final theme = Theme.of(context);
    return Container(
      key: _bucketProfileSummaryKey,
      width: double.infinity,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(14),
        color: theme.colorScheme.surfaceContainerHighest,
        border: Border.all(color: theme.colorScheme.outlineVariant),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            'Selected profile',
            style: theme.textTheme.labelLarge?.copyWith(
              color: theme.colorScheme.onSurfaceVariant,
            ),
          ),
          const SizedBox(height: 8),
          Text(
            profile.name,
            style: theme.textTheme.titleMedium?.copyWith(
              fontWeight: FontWeight.w700,
              color: theme.colorScheme.onSurface,
            ),
          ),
          const SizedBox(height: 8),
          SelectableText(
            profile.endpointUrl,
            style: theme.textTheme.bodyMedium?.copyWith(
              color: theme.colorScheme.onSurface,
            ),
          ),
          const SizedBox(height: 10),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              Chip(
                label: Text(
                  'Region ${profile.region}',
                  style: TextStyle(color: theme.colorScheme.onSurface),
                ),
              ),
              Chip(
                label: Text(
                  profile.verifyTls ? 'TLS verify on' : 'TLS verify off',
                  style: TextStyle(color: theme.colorScheme.onSurface),
                ),
              ),
              Chip(
                label: Text(
                  profile.pathStyle
                      ? 'Path-style addressing'
                      : 'Virtual hosted',
                  style: TextStyle(color: theme.colorScheme.onSurface),
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          widget.inlineStatBuilder(
            'Profile ID',
            profile.id,
          ),
          widget.inlineStatBuilder(
            'Retries and timeouts',
            '${profile.maxAttempts} attempts • ${profile.connectTimeoutSeconds}s connect • ${profile.readTimeoutSeconds}s read',
          ),
        ],
      ),
    );
  }
}
