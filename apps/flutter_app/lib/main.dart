import 'package:flutter/material.dart';

import 'app/s3_browser_app.dart';
import 'services/app_bootstrap.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final controller = await AppBootstrap.initialize();
  runApp(S3BrowserApp(controller: controller));
}
