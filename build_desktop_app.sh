#!/usr/bin/env bash
set -euo pipefail

APP_NAME="OKX Local App"
BUNDLE_ID="com.cc.okxlocalapp"
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/dist"
APP_BUNDLE="$BUILD_DIR/$APP_NAME.app"
DESKTOP_APP="$HOME/Desktop/$APP_NAME.app"
MACOS_DIR="$APP_BUNDLE/Contents/MacOS"
RESOURCES_DIR="$APP_BUNDLE/Contents/Resources"
FRAMEWORKS_DIR="$APP_BUNDLE/Contents/Frameworks"
EXECUTABLE="$MACOS_DIR/$APP_NAME"
BUNDLED_PYTHON_SOURCE="${BUNDLED_PYTHON_SOURCE:-/Library/Frameworks/Python.framework}"
LAUNCH_AGENT_DIR="$HOME/Library/LaunchAgents"
LAUNCH_AGENT_PLIST="$LAUNCH_AGENT_DIR/com.cc.okxlocalapp.desktop.plist"
APP_SUPPORT_DIR="$HOME/Library/Application Support/OKXLocalApp"
RUNTIME_APP_DIR="$APP_SUPPORT_DIR/runtime-app"
RUNTIME_STAMP_FILE="$APP_SUPPORT_DIR/runtime-source.stamp"
LAUNCH_AGENT_LABEL="com.cc.okxlocalapp.desktop"

mkdir -p "$MACOS_DIR" "$RESOURCES_DIR/app" "$FRAMEWORKS_DIR"

cat >"$APP_BUNDLE/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>$APP_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>1.0.0</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>LSApplicationCategoryType</key>
  <string>public.app-category.finance</string>
  <key>LSMinimumSystemVersion</key>
  <string>13.0</string>
  <key>LSMultipleInstancesProhibited</key>
  <true/>
  <key>NSAppTransportSecurity</key>
  <dict>
    <key>NSAllowsLocalNetworking</key>
    <true/>
    <key>NSAllowsArbitraryLoadsInWebContent</key>
    <true/>
  </dict>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
EOF

swiftc \
  -O \
  -framework Cocoa \
  -framework WebKit \
  "$ROOT_DIR/native-mac/main.swift" \
  -o "$EXECUTABLE"

if [ -d "$BUNDLED_PYTHON_SOURCE" ]; then
  rm -rf "$FRAMEWORKS_DIR/Python.framework"
  rsync -a "$BUNDLED_PYTHON_SOURCE/" "$FRAMEWORKS_DIR/Python.framework/"
fi

cp "$ROOT_DIR/server.py" "$RESOURCES_DIR/app/server.py"
rm -rf "$RESOURCES_DIR/app/static"
cp -R "$ROOT_DIR/static" "$RESOURCES_DIR/app/static"
rm -rf "$RESOURCES_DIR/app/scripts"
mkdir -p "$RESOURCES_DIR/app/scripts"
if [ -d "$ROOT_DIR/scripts" ]; then
  cp -R "$ROOT_DIR/scripts/." "$RESOURCES_DIR/app/scripts/"
fi
rm -rf "$RESOURCES_DIR/app/vendor"
mkdir -p "$RESOURCES_DIR/app/vendor"
if [ -d "$ROOT_DIR/vendor/solominer" ]; then
  cp -R "$ROOT_DIR/vendor/solominer" "$RESOURCES_DIR/app/vendor/solominer"
fi
if [ -d "$ROOT_DIR/vendor/MiroFish" ]; then
  cp -R "$ROOT_DIR/vendor/MiroFish" "$RESOURCES_DIR/app/vendor/MiroFish"
  rm -rf "$RESOURCES_DIR/app/vendor/MiroFish/.git"
fi
find "$RESOURCES_DIR/app/vendor" -name '__pycache__' -type d -prune -exec rm -rf {} +

codesign --force --deep --sign - "$APP_BUNDLE" >/dev/null 2>&1 || true

mkdir -p "$DESKTOP_APP"
rsync -a --delete "$APP_BUNDLE/" "$DESKTOP_APP/"

mkdir -p "$LAUNCH_AGENT_DIR" "$APP_SUPPORT_DIR"

rm -rf "$RUNTIME_APP_DIR"
mkdir -p "$RUNTIME_APP_DIR"
cp "$ROOT_DIR/server.py" "$RUNTIME_APP_DIR/server.py"
rm -rf "$RUNTIME_APP_DIR/static"
cp -R "$ROOT_DIR/static" "$RUNTIME_APP_DIR/static"
rm -rf "$RUNTIME_APP_DIR/scripts"
mkdir -p "$RUNTIME_APP_DIR/scripts"
if [ -d "$ROOT_DIR/scripts" ]; then
  cp -R "$ROOT_DIR/scripts/." "$RUNTIME_APP_DIR/scripts/"
fi
rm -rf "$RUNTIME_APP_DIR/vendor"
mkdir -p "$RUNTIME_APP_DIR/vendor"
if [ -d "$ROOT_DIR/vendor/solominer" ]; then
  cp -R "$ROOT_DIR/vendor/solominer" "$RUNTIME_APP_DIR/vendor/solominer"
fi
if [ -d "$ROOT_DIR/vendor/MiroFish" ]; then
  cp -R "$ROOT_DIR/vendor/MiroFish" "$RUNTIME_APP_DIR/vendor/MiroFish"
  rm -rf "$RUNTIME_APP_DIR/vendor/MiroFish/.git"
fi
find "$RUNTIME_APP_DIR/vendor" -name '__pycache__' -type d -prune -exec rm -rf {} +
rm -f "$RUNTIME_STAMP_FILE"

cat >"$LAUNCH_AGENT_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LAUNCH_AGENT_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$DESKTOP_APP/Contents/MacOS/$APP_NAME</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Interactive</string>
  <key>LimitLoadToSessionType</key>
  <array>
    <string>Aqua</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$DESKTOP_APP/Contents/Resources/app</string>
  <key>StandardOutPath</key>
  <string>$APP_SUPPORT_DIR/launchd.stdout.log</string>
  <key>StandardErrorPath</key>
  <string>$APP_SUPPORT_DIR/launchd.stderr.log</string>
</dict>
</plist>
EOF

pkill -f "$DESKTOP_APP/Contents/MacOS/$APP_NAME" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$LAUNCH_AGENT_PLIST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$LAUNCH_AGENT_PLIST" >/dev/null 2>&1 || launchctl load -w "$LAUNCH_AGENT_PLIST" >/dev/null 2>&1 || true
launchctl enable "gui/$(id -u)/$LAUNCH_AGENT_LABEL" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/$(id -u)/$LAUNCH_AGENT_LABEL" >/dev/null 2>&1 || true

echo "$DESKTOP_APP"
