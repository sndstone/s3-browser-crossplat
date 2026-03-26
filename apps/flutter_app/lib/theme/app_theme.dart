import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';

class AppTheme {
  static ThemeData light({required int scalePercent}) {
    const baseScheme = ColorScheme(
      brightness: Brightness.light,
      primary: Color(0xFF0F6C65),
      onPrimary: Color(0xFFF7FBF8),
      primaryContainer: Color(0xFFB6E4DA),
      onPrimaryContainer: Color(0xFF0A3733),
      secondary: Color(0xFFC7862F),
      onSecondary: Color(0xFF2D1B04),
      secondaryContainer: Color(0xFFF5D9AF),
      onSecondaryContainer: Color(0xFF5A3710),
      tertiary: Color(0xFF8C5A2B),
      onTertiary: Color(0xFFFFF8F2),
      tertiaryContainer: Color(0xFFF0D7BF),
      onTertiaryContainer: Color(0xFF46270A),
      error: Color(0xFFB3261E),
      onError: Colors.white,
      errorContainer: Color(0xFFF9DEDC),
      onErrorContainer: Color(0xFF410E0B),
      surface: Color(0xFFFFFCF5),
      onSurface: Color(0xFF182421),
      surfaceContainerHighest: Color(0xFFE8E3D4),
      onSurfaceVariant: Color(0xFF52615D),
      outline: Color(0xFF8E9C97),
      outlineVariant: Color(0xFFD0D9D4),
      shadow: Color(0x290C1715),
      scrim: Color(0x66000000),
      inverseSurface: Color(0xFF1E2C29),
      onInverseSurface: Color(0xFFF3F6F2),
      inversePrimary: Color(0xFF92D1C5),
      surfaceTint: Color(0xFF0F6C65),
    );
    return _themeFromScheme(
      scheme: baseScheme,
      scalePercent: scalePercent,
      scaffoldBackground: const Color(0xFFF4EFE3),
      railBackground: const Color(0xFFE0E8E2),
      navigationBarBackground: const Color(0xFFF9F4E8),
      cardColor: Colors.white,
    );
  }

  static ThemeData dark({required int scalePercent}) {
    const baseScheme = ColorScheme(
      brightness: Brightness.dark,
      primary: Color(0xFF8FD0C3),
      onPrimary: Color(0xFF00201D),
      primaryContainer: Color(0xFF134C47),
      onPrimaryContainer: Color(0xFFB6E4DA),
      secondary: Color(0xFFF1C986),
      onSecondary: Color(0xFF442B07),
      secondaryContainer: Color(0xFF654113),
      onSecondaryContainer: Color(0xFFF5D9AF),
      tertiary: Color(0xFFE6C29A),
      onTertiary: Color(0xFF4D2C0A),
      tertiaryContainer: Color(0xFF6A4318),
      onTertiaryContainer: Color(0xFFF0D7BF),
      error: Color(0xFFF2B8B5),
      onError: Color(0xFF601410),
      errorContainer: Color(0xFF8C1D18),
      onErrorContainer: Color(0xFFF9DEDC),
      surface: Color(0xFF0D1716),
      onSurface: Color(0xFFE8F2EE),
      surfaceContainerHighest: Color(0xFF1A2B29),
      onSurfaceVariant: Color(0xFFB9C9C2),
      outline: Color(0xFF889690),
      outlineVariant: Color(0xFF344643),
      shadow: Color(0x66000000),
      scrim: Color(0x99000000),
      inverseSurface: Color(0xFFE8F2EE),
      onInverseSurface: Color(0xFF15211F),
      inversePrimary: Color(0xFF0F6C65),
      surfaceTint: Color(0xFF8FD0C3),
    );
    return _themeFromScheme(
      scheme: baseScheme,
      scalePercent: scalePercent,
      scaffoldBackground: const Color(0xFF081110),
      railBackground: const Color(0xFF0C1B19),
      navigationBarBackground: const Color(0xFF0F1A18),
      cardColor: const Color(0xFF12201E),
    );
  }

  static ThemeData _themeFromScheme({
    required ColorScheme scheme,
    required int scalePercent,
    required Color scaffoldBackground,
    required Color railBackground,
    required Color navigationBarBackground,
    required Color cardColor,
  }) {
    final base = ThemeData(
      useMaterial3: true,
      colorScheme: scheme,
      visualDensity: _visualDensity(scalePercent),
      materialTapTargetSize: MaterialTapTargetSize.padded,
    );
    final textTheme = GoogleFonts.spaceGroteskTextTheme(base.textTheme).copyWith(
      displayLarge: GoogleFonts.sora(
        fontWeight: FontWeight.w700,
        letterSpacing: -1.2,
        fontSize: 58,
        height: 0.98,
        color: scheme.onSurface,
      ),
      displayMedium: GoogleFonts.sora(
        fontWeight: FontWeight.w700,
        letterSpacing: -0.9,
        fontSize: 44,
        height: 1.0,
        color: scheme.onSurface,
      ),
      headlineMedium: GoogleFonts.sora(
        fontWeight: FontWeight.w700,
        letterSpacing: -0.6,
        color: scheme.onSurface,
      ),
      headlineSmall: GoogleFonts.sora(
        fontWeight: FontWeight.w700,
        letterSpacing: -0.4,
        color: scheme.onSurface,
      ),
      titleLarge: GoogleFonts.spaceGrotesk(
        fontWeight: FontWeight.w700,
        letterSpacing: -0.3,
        color: scheme.onSurface,
      ),
      titleMedium: GoogleFonts.spaceGrotesk(
        fontWeight: FontWeight.w600,
        color: scheme.onSurface,
      ),
      bodyLarge: GoogleFonts.inter(
        fontWeight: FontWeight.w500,
        color: scheme.onSurface,
        height: 1.45,
      ),
      bodyMedium: GoogleFonts.inter(
        fontWeight: FontWeight.w500,
        color: scheme.onSurface,
        height: 1.45,
      ),
      bodySmall: GoogleFonts.inter(
        fontWeight: FontWeight.w500,
        color: scheme.onSurfaceVariant,
        height: 1.35,
      ),
      labelLarge: GoogleFonts.spaceGrotesk(
        fontWeight: FontWeight.w700,
        letterSpacing: 0.1,
      ),
    );

    return base.copyWith(
      textTheme: textTheme,
      scaffoldBackgroundColor: scaffoldBackground,
      canvasColor: scaffoldBackground,
      cardTheme: CardThemeData(
        color: cardColor,
        elevation: 0,
        clipBehavior: Clip.antiAlias,
        margin: EdgeInsets.zero,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(28),
          side: BorderSide(
            color: scheme.outlineVariant.withValues(alpha: 0.8),
          ),
        ),
      ),
      dividerTheme: DividerThemeData(
        color: scheme.outlineVariant.withValues(alpha: 0.8),
        thickness: 1,
      ),
      navigationRailTheme: NavigationRailThemeData(
        backgroundColor: railBackground,
        selectedIconTheme: IconThemeData(color: scheme.onPrimaryContainer),
        selectedLabelTextStyle: textTheme.labelLarge?.copyWith(
          color: scheme.onSurface,
        ),
      ),
      navigationBarTheme: NavigationBarThemeData(
        backgroundColor: navigationBarBackground,
        indicatorColor: scheme.primaryContainer,
        labelTextStyle: WidgetStateProperty.resolveWith(
          (states) => textTheme.labelLarge?.copyWith(
            color: states.contains(WidgetState.selected)
                ? scheme.onSurface
                : scheme.onSurfaceVariant,
          ),
        ),
      ),
      inputDecorationTheme: InputDecorationTheme(
        filled: true,
        fillColor: scheme.surface,
        contentPadding: const EdgeInsets.symmetric(
          horizontal: 18,
          vertical: 16,
        ),
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(20),
          borderSide: BorderSide(color: scheme.outlineVariant),
        ),
        enabledBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(20),
          borderSide: BorderSide(color: scheme.outlineVariant),
        ),
        focusedBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(20),
          borderSide: BorderSide(color: scheme.primary, width: 1.4),
        ),
      ),
      chipTheme: base.chipTheme.copyWith(
        backgroundColor: scheme.secondaryContainer.withValues(alpha: 0.65),
        side: BorderSide.none,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(999),
        ),
        labelStyle: textTheme.labelLarge?.copyWith(color: scheme.onSurface),
      ),
      filledButtonTheme: FilledButtonThemeData(
        style: FilledButton.styleFrom(
          backgroundColor: scheme.primary,
          foregroundColor: scheme.onPrimary,
          padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 16),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(22),
          ),
          textStyle: textTheme.labelLarge,
        ),
      ),
      outlinedButtonTheme: OutlinedButtonThemeData(
        style: OutlinedButton.styleFrom(
          padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 16),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(22),
          ),
          side: BorderSide(color: scheme.outlineVariant),
          textStyle: textTheme.labelLarge,
        ),
      ),
      segmentedButtonTheme: SegmentedButtonThemeData(
        style: ButtonStyle(
          textStyle: WidgetStatePropertyAll(textTheme.labelLarge),
          padding: const WidgetStatePropertyAll(
            EdgeInsets.symmetric(horizontal: 16, vertical: 14),
          ),
        ),
      ),
      listTileTheme: ListTileThemeData(
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(22),
        ),
        tileColor: scheme.surface.withValues(alpha: 0.4),
        contentPadding: const EdgeInsets.symmetric(
          horizontal: 16,
          vertical: 4,
        ),
      ),
      tabBarTheme: TabBarThemeData(
        labelColor: scheme.onSurface,
        unselectedLabelColor: scheme.onSurfaceVariant,
        indicator: BoxDecoration(
          borderRadius: BorderRadius.circular(999),
          color: scheme.primaryContainer,
        ),
        labelStyle: textTheme.labelLarge,
        unselectedLabelStyle: textTheme.labelLarge,
        dividerColor: Colors.transparent,
      ),
    );
  }

  static VisualDensity _visualDensity(int scalePercent) {
    final offset = ((scalePercent - 100) / 12).clamp(-2.5, 1.5).toDouble();
    return VisualDensity(horizontal: offset, vertical: offset);
  }
}
