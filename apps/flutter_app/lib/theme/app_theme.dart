import 'package:flutter/material.dart';

class AppTheme {
  static ThemeData light({required int scalePercent}) {
    const seed = Color(0xFF0F766E);
    final base = ThemeData(
      useMaterial3: true,
      visualDensity: _visualDensity(scalePercent),
      materialTapTargetSize: MaterialTapTargetSize.shrinkWrap,
      colorScheme: ColorScheme.fromSeed(
        seedColor: seed,
        brightness: Brightness.light,
      ),
    );

    return base.copyWith(
      scaffoldBackgroundColor: const Color(0xFFF4F5EF),
      cardTheme: const CardThemeData(
        elevation: 0,
        color: Colors.white,
        margin: EdgeInsets.zero,
      ),
      navigationRailTheme: const NavigationRailThemeData(
        backgroundColor: Color(0xFFE7ECDF),
      ),
      inputDecorationTheme: const InputDecorationTheme(
        border: OutlineInputBorder(),
        filled: true,
        fillColor: Colors.white,
      ),
    );
  }

  static ThemeData dark({required int scalePercent}) {
    const seed = Color(0xFF7DD3C7);
    final base = ThemeData(
      useMaterial3: true,
      visualDensity: _visualDensity(scalePercent),
      materialTapTargetSize: MaterialTapTargetSize.shrinkWrap,
      colorScheme: ColorScheme.fromSeed(
        seedColor: seed,
        brightness: Brightness.dark,
      ),
    );

    return base.copyWith(
      scaffoldBackgroundColor: const Color(0xFF101514),
      cardTheme: const CardThemeData(
        elevation: 0,
        color: Color(0xFF16201D),
        margin: EdgeInsets.zero,
      ),
      navigationRailTheme: const NavigationRailThemeData(
        backgroundColor: Color(0xFF0E1715),
      ),
    );
  }

  static VisualDensity _visualDensity(int scalePercent) {
    final offset = ((scalePercent - 100) / 10).clamp(-3, 2).toDouble();
    return VisualDensity(horizontal: offset, vertical: offset);
  }
}
