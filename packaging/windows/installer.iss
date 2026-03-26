[Setup]
AppName=S3 Browser Crossplat
AppVersion=2.0.10
DefaultDirName={autopf}\S3 Browser Crossplat
DefaultGroupName=S3 Browser Crossplat
OutputBaseFilename=s3-browser-crossplat-installer

[Files]
Source: "..\..\apps\flutter_app\build\windows\x64\runner\Release\*"; DestDir: "{app}"; Flags: recursesubdirs
