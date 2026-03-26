param(
    [ValidateSet("windows", "android")]
    [string]$Platform = "windows",
    [ValidateSet("x64", "arm64")]
    [string]$Arch = "x64",
    [ValidateSet("none", "msi")]
    [string]$Installer = "msi",
    [switch]$IncludeEngineToolchains,
    [switch]$OpenDeveloperSettings,
    [switch]$Help,
    [switch]$KeepShellOpen
)

$ErrorActionPreference = "Stop"
$ScriptPath = $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent (Split-Path -Parent $ScriptPath)
$ToolCacheDir = Join-Path $RootDir ".tmp\toolchains"

function Get-WindowsSystemPath {
    $SystemRoot = if ($env:SystemRoot) { $env:SystemRoot } else { "C:\Windows" }
    return @(
        (Join-Path $SystemRoot "System32"),
        $SystemRoot
    ) -join ";"
}

function Exit-Build {
    param(
        [int]$Code = 0
    )

    if ($KeepShellOpen) {
        Write-Host ""
        if ($Code -eq 0) {
            Write-Host "Build finished. Press Enter to close this elevated window."
        } else {
            Write-Host "Build ended with exit code $Code. Press Enter to close this elevated window."
        }
        [void](Read-Host)
    }

    exit $Code
}

trap {
    Write-Host $_ -ForegroundColor Red
    Exit-Build 1
}

function Test-IsAdministrator {
    $CurrentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $Principal = New-Object Security.Principal.WindowsPrincipal($CurrentIdentity)
    return $Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-SymlinkSupport {
    $ProbeRoot = Join-Path $env:TEMP "s3-browser-symlink-test"
    $TargetDir = Join-Path $ProbeRoot "target"
    $LinkDir = Join-Path $ProbeRoot "link"

    if (Test-Path $ProbeRoot) {
        Remove-Item -Recurse -Force $ProbeRoot
    }

    New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null

    try {
        New-Item -ItemType SymbolicLink -Path $LinkDir -Target $TargetDir -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    } finally {
        if (Test-Path $ProbeRoot) {
            Remove-Item -Recurse -Force $ProbeRoot -ErrorAction SilentlyContinue
        }
    }
}

function Get-BuildProcessArguments {
    $ArgumentList = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $ScriptPath,
        "-Platform", $Platform,
        "-Arch", $Arch
    )

    if ($Platform -eq "windows") {
        $ArgumentList += @("-Installer", $Installer)
    }
    if ($IncludeEngineToolchains) {
        $ArgumentList += "-IncludeEngineToolchains"
    }
    if ($OpenDeveloperSettings) {
        $ArgumentList += "-OpenDeveloperSettings"
    }
    if ($Help) {
        $ArgumentList += "-Help"
    }
    if (-not $ArgumentList.Contains("-KeepShellOpen")) {
        $ArgumentList += "-KeepShellOpen"
    }

    return $ArgumentList
}

function Ensure-WindowsSymlinkSupport {
    if ($Platform -ne "windows") {
        return
    }

    if ($OpenDeveloperSettings) {
        Start-Process "ms-settings:developers"
    }

    if (Test-SymlinkSupport) {
        return
    }

    if (-not (Test-IsAdministrator)) {
        Write-Host "Symlink support is unavailable in the current session. Relaunching the Windows build with elevation..."
        Write-Host "The elevated PowerShell window will stay open when the build ends so you can review the output."
        try {
            $ElevatedProcess = Start-Process `
                -FilePath "powershell.exe" `
                -ArgumentList (Get-BuildProcessArguments) `
                -Verb RunAs `
                -WorkingDirectory (Get-Location).Path `
                -Wait `
                -PassThru
            Exit-Build $ElevatedProcess.ExitCode
        } catch [System.ComponentModel.Win32Exception] {
            if ($_.Exception.NativeErrorCode -eq 1223) {
                Write-Error @"
Windows desktop Flutter builds with plugins require symlink support.

The build tried to relaunch itself with elevation, but the UAC prompt was cancelled.

Fix one of these first:
  1. Accept the elevation prompt when it appears
  2. Enable Developer Mode:
     start ms-settings:developers

Then rerun:
  .\scripts\build.ps1 -Platform windows
"@
                Exit-Build 1
            }
            throw
        }
    }

    Write-Error @"
Windows desktop Flutter builds with plugins require symlink support.

The build is already running elevated, but Windows still refused to create a test symlink.

Fix one of these first:
  1. Enable Developer Mode:
     start ms-settings:developers
  2. Or verify local policy still allows administrators to create symbolic links

Then rerun:
  .\scripts\build.ps1 -Platform windows
"@
    Exit-Build 1
}

function Resolve-ToolsDir {
    param(
        [string]$TargetPath
    )

    if (-not (Test-Path $TargetPath)) {
        New-Item -ItemType Directory -Force -Path $TargetPath | Out-Null
    }

    $ResolvedTarget = (Resolve-Path $TargetPath).Path
    try {
        $SubstOutput = & subst.exe 2>$null
        foreach ($Line in $SubstOutput) {
            if ($Line -match '^([A-Z]:)\\: => (.+)$') {
                $MappedPath = $matches[2].Trim()
                if ($MappedPath -eq $ResolvedTarget) {
                    return "$($matches[1])\."
                }
            }
        }

        foreach ($Letter in @('S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')) {
            $DriveName = "${Letter}:"
            if (-not (Get-PSDrive -Name $Letter -ErrorAction SilentlyContinue)) {
                & subst.exe $DriveName $ResolvedTarget | Out-Null
                return "$DriveName\."
            }
        }
    } catch {
        return $ResolvedTarget
    }

    return $ResolvedTarget
}

if ($Help) {
    Write-Host "Usage:"
    Write-Host "  .\scripts\build.ps1"
    Write-Host "  .\scripts\build.ps1 -Platform windows"
    Write-Host "  .\scripts\build.ps1 -Platform android"
    Write-Host "  .\scripts\build.ps1 -Platform windows -Arch arm64"
    Write-Host "  .\scripts\build.ps1 -Platform windows -Installer none"
    Write-Host "  .\scripts\build.ps1 -Platform windows -IncludeEngineToolchains"
    Write-Host "  .\scripts\build.ps1 -Platform windows -OpenDeveloperSettings"
    Write-Host ""
    Write-Host "Platforms:"
    Write-Host "  windows  Build the Windows desktop app (default)"
    Write-Host "  android  Build the sideloadable arm64 APK and the secondary App Bundle"
    Write-Host ""
    Write-Host "Switches:"
    Write-Host "  -Installer msi|none       Build a Windows MSI after the desktop app build (default: msi)"
    Write-Host "  -IncludeEngineToolchains  Also download Python, Go, Java, and Rust toolchains for non-Windows targets"
    Write-Host "  -OpenDeveloperSettings    Open the Windows Developer Mode settings page before building"
    Write-Host ""
    Write-Host "Windows builds relaunch themselves elevated if symlink support is unavailable."
    Write-Host "Android builds stage an SDK under .tmp\toolchains\android-sdk, sign the release output with the debug key, and emit app-arm64-v8a-release.apk for sideloading."
    Exit-Build 0
}

Ensure-WindowsSymlinkSupport

$BootstrapComponents = @("flutter")
if ($Platform -eq "windows") {
    $BootstrapComponents += @("python", "go", "java", "rust")
}
if ($Platform -eq "android") {
    $BootstrapComponents += @("java", "android")
}
if ($IncludeEngineToolchains) {
    $BootstrapComponents += @("python", "go", "java", "rust")
}
if ($Platform -eq "windows" -and $Installer -eq "msi") {
    $BootstrapComponents += "wix"
}
$BootstrapComponents = $BootstrapComponents | Select-Object -Unique

& (Join-Path $RootDir "scripts\bootstrap.ps1") -Components $BootstrapComponents -Arch $Arch

$ToolsDir = Resolve-ToolsDir -TargetPath $ToolCacheDir

$env:CARGO_HOME = Join-Path $ToolsDir "cargo"
$env:RUSTUP_HOME = Join-Path $ToolsDir "rustup"
$env:PATH = "$(Get-WindowsSystemPath);$(Join-Path $ToolsDir 'flutter\bin');$(Join-Path $ToolsDir 'go\bin');$(Join-Path $ToolsDir 'java\bin');$(Join-Path $ToolsDir 'cargo\bin');$(Join-Path $ToolsDir 'wix');$(Join-Path $ToolsDir 'wix\bin');$(Join-Path $ToolsDir 'android-sdk\cmdline-tools\latest\bin');$(Join-Path $ToolsDir 'android-sdk\platform-tools');$env:PATH"
$env:JAVA_HOME = Join-Path $ToolsDir "java"
$env:ANDROID_SDK_ROOT = Join-Path $ToolsDir "android-sdk"
$env:ANDROID_HOME = $env:ANDROID_SDK_ROOT
$FlutterDir = Join-Path $RootDir "apps\flutter_app"
$LogDir = Join-Path $RootDir ".tmp\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$AnalyzeLog = Join-Path $LogDir "flutter-analyze-$Platform-$Arch.log"
$BuildLog = Join-Path $LogDir "flutter-build-$Platform-$Arch.log"

function Get-AppVersion {
    $PubspecPath = Join-Path $FlutterDir "pubspec.yaml"
    $Version = "2.0.10"
    if (Test-Path $PubspecPath) {
        $VersionMatch = Select-String -Path $PubspecPath -Pattern '^version:\s*([0-9]+\.[0-9]+\.[0-9]+)'
        if ($VersionMatch) {
            $Version = $VersionMatch.Matches[0].Groups[1].Value
        }
    }
    return $Version
}

function New-ShortJunction {
    param(
        [string]$LinkPath,
        [string]$TargetPath
    )

    if (Test-Path $LinkPath) {
        Remove-Item -Force -Recurse $LinkPath
    }

    New-Item -ItemType Junction -Path $LinkPath -Target $TargetPath | Out-Null
    return $LinkPath
}

$ShortFlutterDir = New-ShortJunction `
    -LinkPath (Join-Path $ToolsDir "app") `
    -TargetPath $FlutterDir

function Ensure-FlutterProject {
    param(
        [string[]]$Platforms
    )

    $NeedsCreate = $false
    foreach ($PlatformName in $Platforms) {
        $PlatformDir = Join-Path $FlutterDir $PlatformName
        if (-not (Test-Path $PlatformDir)) {
            $NeedsCreate = $true
            break
        }
    }

    $MetadataFile = Join-Path $FlutterDir ".metadata"
    if (-not (Test-Path $MetadataFile)) {
        $NeedsCreate = $true
    }

    if ($NeedsCreate) {
        $PlatformList = $Platforms -join ","
        Write-Host "Creating missing Flutter platform scaffolding: $PlatformList"
        Push-Location $FlutterDir
        & flutter create `
            --project-name s3_browser_crossplat `
            --org com.example.s3browser `
            --platforms $PlatformList `
            .
        Pop-Location
    }
}

function Ensure-AndroidBuildEnvironment {
    $AndroidRoot = Join-Path $ToolsDir "android-sdk"
    $AndroidLocalProperties = Join-Path $FlutterDir "android\local.properties"
    $FlutterSdk = Join-Path $ToolsDir "flutter"

    if (-not (Test-Path (Join-Path $AndroidRoot "cmdline-tools\latest\bin\sdkmanager.bat"))) {
        throw "Android SDK command-line tools were not found at $AndroidRoot. Rerun .\scripts\bootstrap.ps1 -Components flutter,java,android."
    }

    & flutter config --android-sdk $AndroidRoot | Out-Null

    $AndroidRootEscaped = $AndroidRoot.Replace('\', '\\')
    $FlutterSdkEscaped = $FlutterSdk.Replace('\', '\\')
    Set-Content -Path $AndroidLocalProperties -Value @(
        "sdk.dir=$AndroidRootEscaped"
        "flutter.sdk=$FlutterSdkEscaped"
    )
}

function Stop-StaleWindowsBuildProcesses {
    $Stopped = $false

    $Processes = Get-Process -Name "s3_browser_crossplat" -ErrorAction SilentlyContinue
    foreach ($Process in $Processes) {
        Write-Host "Stopping running S3 Browser Crossplat process (PID $($Process.Id))"
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        $Stopped = $true
    }

    if (-not $Stopped) {
        & cmd.exe /c "taskkill /IM s3_browser_crossplat.exe /F >nul 2>&1"
        $global:LASTEXITCODE = 0
    }
}

function Remove-PathWithRetries {
    param(
        [string]$Path,
        [int]$Attempts = 5
    )

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        if (-not (Test-Path $Path)) {
            return $true
        }

        try {
            Remove-Item -Recurse -Force $Path -ErrorAction Stop
            return $true
        } catch {
            if ($Attempt -eq $Attempts) {
                return $false
            }
            Start-Sleep -Milliseconds (250 * $Attempt)
        }
    }

    return -not (Test-Path $Path)
}

Push-Location $ShortFlutterDir
if ($Platform -eq "windows") {
    Stop-StaleWindowsBuildProcesses

    $BuildDir = Join-Path $FlutterDir "build"
    if (-not (Remove-PathWithRetries -Path $BuildDir)) {
        Write-Host "Unable to fully remove existing build directory before flutter clean. Continuing with Flutter cleanup."
    }
    Ensure-FlutterProject -Platforms @("windows")
    & flutter clean
} elseif ($Platform -eq "android") {
    Ensure-FlutterProject -Platforms @("android")
    Ensure-AndroidBuildEnvironment
}
& flutter pub get
if ($LASTEXITCODE -ne 0) {
    Pop-Location
    Exit-Build $LASTEXITCODE
}

if ($Platform -eq "windows") {
    Write-Host "Running flutter analyze..."
    & flutter analyze 2>&1 | Tee-Object -FilePath $AnalyzeLog
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Analyze log written to $AnalyzeLog"
        Pop-Location
        Exit-Build $LASTEXITCODE
    }
}

if ($Platform -eq "windows") {
        Write-Host "Running verbose Flutter Windows build for $Arch..."
    $BuildArtifactsToClear = @(
        (Join-Path $ShortFlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.exe"),
        (Join-Path $ShortFlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.pdb"),
        (Join-Path $ShortFlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.lib"),
        (Join-Path $FlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.exe"),
        (Join-Path $FlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.pdb"),
        (Join-Path $FlutterDir "build\\windows\\$Arch\\runner\\Release\\s3_browser_crossplat.lib")
    )
    foreach ($Artifact in $BuildArtifactsToClear) {
        if (Test-Path $Artifact) {
            [void](Remove-PathWithRetries -Path $Artifact)
        }
    }
    & flutter build windows -v 2>&1 | Tee-Object -FilePath $BuildLog
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build log written to $BuildLog"
        Pop-Location
        Exit-Build $LASTEXITCODE
    }
    Pop-Location
    & (Join-Path $RootDir "scripts\stage-engines.ps1") `
        -ReleaseDir (Join-Path $FlutterDir "build\windows\$Arch\runner\Release") `
        -ToolsDir $ToolsDir `
        -Arch $Arch
    if ($LASTEXITCODE -ne 0) {
        Exit-Build $LASTEXITCODE
    }
    if ($Installer -eq "msi") {
        & (Join-Path $RootDir "scripts\package-windows.ps1") -Arch $Arch
        Exit-Build $LASTEXITCODE
    }
    Exit-Build 0
} elseif ($Platform -eq "android") {
    Write-Host "Building Android APK (arm64, sideloadable release signed with the debug key)..."
    & flutter build apk --release --target-platform android-arm64 --split-per-abi 2>&1 | Tee-Object -FilePath $BuildLog
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build log written to $BuildLog"
        Pop-Location
        Exit-Build $LASTEXITCODE
    }
    Write-Host "Building Android App Bundle (secondary artifact)..."
    & flutter build appbundle --release --target-platform android-arm64 2>&1 | Tee-Object -FilePath $BuildLog -Append
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build log written to $BuildLog"
        Pop-Location
        Exit-Build $LASTEXITCODE
    }
    $Version = Get-AppVersion
    $AndroidArtifactDir = Join-Path $RootDir "dist\android"
    $SourceApk = Join-Path $FlutterDir "build\app\outputs\flutter-apk\app-arm64-v8a-release.apk"
    $SourceAab = Join-Path $FlutterDir "build\app\outputs\bundle\release\app-release.aab"
    $VersionedApk = Join-Path $AndroidArtifactDir "s3-browser-crossplat-android-$Version-arm64.apk"
    $VersionedAab = Join-Path $AndroidArtifactDir "s3-browser-crossplat-android-$Version-arm64.aab"
    New-Item -ItemType Directory -Force -Path $AndroidArtifactDir | Out-Null
    if (Test-Path $SourceApk) {
        Copy-Item -Path $SourceApk -Destination $VersionedApk -Force
    }
    if (Test-Path $SourceAab) {
        Copy-Item -Path $SourceAab -Destination $VersionedAab -Force
    }
    Write-Host "Primary APK artifact: $VersionedApk"
    Write-Host "Secondary AAB artifact: $VersionedAab"
}

Pop-Location
