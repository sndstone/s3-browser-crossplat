[Setup]
AppName=S3 Browser Cross Platform
AppVersion=2.0.8
DefaultDirName={autopf}\S3 Browser Cross Platform
DefaultGroupName=S3 Browser Cross Platform
OutputBaseFilename=s3-browser-crossplat-installer

[Files]
Source: "..\..\apps\flutter_app\build\windows\x64\runner\Release\*"; DestDir: "{app}"; Flags: recursesubdirs
