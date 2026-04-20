import Cocoa
import CryptoKit
import Foundation
import WebKit

final class AppDelegate: NSObject, NSApplicationDelegate, WKNavigationDelegate {
    private let appName = "OKX Local App"
    private let healthPorts = [8765]
    private let healthPath = "/api/ping"
    private let compatibilityPaths = [
        "/api/local-config"
    ]
    private let bundleId = "com.cc.okxlocalapp"

    private var window: NSWindow!
    private var webView: WKWebView!
    private var serverProcess: Process?
    private var serverOutputHandle: FileHandle?
    private var serverLogURL: URL?
    private var attachedToExistingServer = false
    private var currentRuntimeSyncStamp = ""
    private var currentServiceURL: URL?

    private struct PythonRuntime {
        let executable: String
        let home: String?
        let version: String?
    }

    private struct RuntimeAppSource {
        let sourceURL: URL
        let runtimeURL: URL
        let label: String
        let stamp: String
        let didSync: Bool
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        if activateExistingInstanceIfNeeded() {
            NSApp.terminate(nil)
            return
        }

        buildWindow()
        showPlaceholder(title: "Starting Local Desk", detail: "Preparing the embedded OKX service.")
        ensureServerAndLoad()
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        if !flag {
            window.makeKeyAndOrderFront(nil)
            if let url = currentServiceURL ?? detectExistingServiceURL(expectedRuntimeSyncStamp: currentRuntimeSyncStamp.isEmpty ? nil : currentRuntimeSyncStamp) {
                load(url)
            }
        }
        NSApp.activate(ignoringOtherApps: true)
        return true
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationWillTerminate(_ notification: Notification) {
        guard !attachedToExistingServer, let process = serverProcess, process.isRunning else {
            serverOutputHandle?.closeFile()
            return
        }
        process.terminate()
        serverOutputHandle?.closeFile()
    }

    private func activateExistingInstanceIfNeeded() -> Bool {
        let currentPid = ProcessInfo.processInfo.processIdentifier
        let others = NSRunningApplication
            .runningApplications(withBundleIdentifier: bundleId)
            .filter { $0.processIdentifier != currentPid }

        guard let existing = others.first else {
            return false
        }

        existing.activate(options: [.activateAllWindows])
        return true
    }

    private func buildWindow() {
        let frame = NSRect(x: 0, y: 0, width: 1440, height: 920)
        window = NSWindow(
            contentRect: frame,
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = appName
        window.center()
        window.isReleasedWhenClosed = false
        window.minSize = NSSize(width: 1120, height: 760)

        let config = WKWebViewConfiguration()
        config.defaultWebpagePreferences.allowsContentJavaScript = true
        config.websiteDataStore = .nonPersistent()
        webView = WKWebView(frame: frame, configuration: config)
        webView.navigationDelegate = self
        webView.setValue(false, forKey: "drawsBackground")

        window.contentView = webView
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    private func ensureServerAndLoad() {
        let preparedRuntimeSource: RuntimeAppSource
        do {
            guard let resourceURL = Bundle.main.resourceURL else {
                showPlaceholder(title: "Missing Resources", detail: "Cannot locate the app bundle resources.")
                return
            }
            preparedRuntimeSource = try prepareRuntimeAppSource(
                bundleAppDir: resourceURL.appendingPathComponent("app", isDirectory: true)
            )
            currentRuntimeSyncStamp = preparedRuntimeSource.stamp
        } catch {
            showPlaceholder(title: "Runtime Sync Failed", detail: error.localizedDescription)
            return
        }

        if preparedRuntimeSource.didSync {
            _ = terminateManagedEmbeddedServices(runtimeSource: preparedRuntimeSource)
        }

        if let url = detectExistingServiceURL(expectedRuntimeSyncStamp: preparedRuntimeSource.stamp), !preparedRuntimeSource.didSync {
            attachedToExistingServer = true
            load(url)
            return
        }

        if detectExistingServiceURL(expectedRuntimeSyncStamp: nil) != nil {
            _ = terminateManagedEmbeddedServices(runtimeSource: preparedRuntimeSource)
        }

        launchEmbeddedServer(runtimeSource: preparedRuntimeSource)

        DispatchQueue.global(qos: .userInitiated).async {
            for _ in 0..<80 {
                if let url = self.detectExistingServiceURL(expectedRuntimeSyncStamp: preparedRuntimeSource.stamp) {
                    DispatchQueue.main.async {
                        self.load(url)
                    }
                    return
                }
                Thread.sleep(forTimeInterval: 0.25)
            }

            let detail = self.readProcessOutput().ifEmpty("The embedded service did not become ready in time.")
            DispatchQueue.main.async {
                self.showPlaceholder(title: "Launch Failed", detail: detail)
            }
        }
    }

    private func launchEmbeddedServer(runtimeSource: RuntimeAppSource) {
        let appResourceDir = runtimeSource.runtimeURL
        let serverPath = appResourceDir.appendingPathComponent("server.py").path
        let dataDir = applicationSupportDirectory().appendingPathComponent("data", isDirectory: true)
        let logURL = applicationSupportDirectory().appendingPathComponent("embedded-server.log", isDirectory: false)

        try? FileManager.default.createDirectory(at: dataDir, withIntermediateDirectories: true)
        FileManager.default.createFile(atPath: logURL.path, contents: Data())

        guard let pythonRuntime = preferredPythonRuntime() else {
            showPlaceholder(
                title: "Python Runtime Missing",
                detail: "The app could not find a bundled Python runtime or a system Python with requests."
            )
            return
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: pythonRuntime.executable)
        process.arguments = [serverPath]
        process.currentDirectoryURL = appResourceDir

        var env = ProcessInfo.processInfo.environment
        env["OKX_LOCAL_APP_DATA_DIR"] = dataDir.path
        env["OKX_LOCAL_APP_RUNTIME_SYNC_STAMP"] = runtimeSource.stamp
        env["OKX_LOCAL_APP_RUNTIME_SOURCE_LABEL"] = runtimeSource.label
        env["PYTHONUNBUFFERED"] = "1"
        if let home = pythonRuntime.home {
            env["PYTHONHOME"] = home
            let pythonLibVersion = pythonRuntime.version ?? "3.12"
            env["PYTHONPATH"] = "\(home)/lib/python\(pythonLibVersion):\(home)/lib/python\(pythonLibVersion)/site-packages"
        }
        process.environment = env

        do {
            let outputHandle = try FileHandle(forWritingTo: logURL)
            try outputHandle.truncate(atOffset: 0)
            serverOutputHandle = outputHandle
            serverLogURL = logURL
            process.standardOutput = outputHandle
            process.standardError = outputHandle
        } catch {
            showPlaceholder(title: "Log Setup Error", detail: error.localizedDescription)
            return
        }

        do {
            if let outputHandle = serverOutputHandle,
               let message = "[runtime] python=\(pythonRuntime.executable) version=\(pythonRuntime.version ?? "system") source=\(runtimeSource.label)\n".data(using: .utf8) {
                outputHandle.write(message)
            }
            try process.run()
            serverProcess = process
        } catch {
            showPlaceholder(title: "Server Launch Error", detail: error.localizedDescription)
        }
    }

    private func prepareRuntimeAppSource(bundleAppDir: URL) throws -> RuntimeAppSource {
        let fileManager = FileManager.default
        let runtimeDir = applicationSupportDirectory().appendingPathComponent("runtime-app", isDirectory: true)
        let source = preferredWorkspaceAppSource() ?? bundleAppDir
        let sourceStamp = try runtimeSourceStamp(for: source)
        let stampURL = applicationSupportDirectory().appendingPathComponent("runtime-source.stamp", isDirectory: false)
        let previousStamp = try? String(contentsOf: stampURL, encoding: .utf8)
        let runtimeServerPath = runtimeDir.appendingPathComponent("server.py").path
        let needsSync = previousStamp != sourceStamp || !fileManager.fileExists(atPath: runtimeServerPath)
        try fileManager.createDirectory(at: runtimeDir, withIntermediateDirectories: true)
        if needsSync {
            try syncRuntimeApp(sourceURL: source, runtimeURL: runtimeDir)
            try sourceStamp.write(to: stampURL, atomically: true, encoding: .utf8)
        }
        let label = source == bundleAppDir ? "bundle" : source.path
        return RuntimeAppSource(sourceURL: source, runtimeURL: runtimeDir, label: label, stamp: sourceStamp, didSync: needsSync)
    }

    private func preferredWorkspaceAppSource() -> URL? {
        let fileManager = FileManager.default
        let candidates = [
            ProcessInfo.processInfo.environment["OKX_LOCAL_APP_SOURCE_DIR"],
            fileManager.homeDirectoryForCurrentUser
                .appendingPathComponent("Documents/New project/okx-local-app", isDirectory: true)
                .path,
        ].compactMap { $0 }

        for path in candidates {
            let url = URL(fileURLWithPath: path, isDirectory: true)
            if isValidAppSource(url) {
                return url
            }
        }
        return nil
    }

    private func isValidAppSource(_ url: URL) -> Bool {
        let fileManager = FileManager.default
        let server = url.appendingPathComponent("server.py")
        let staticDir = url.appendingPathComponent("static", isDirectory: true)
        return fileManager.fileExists(atPath: server.path) && fileManager.fileExists(atPath: staticDir.path)
    }

    private func syncRuntimeApp(sourceURL: URL, runtimeURL: URL) throws {
        let fileManager = FileManager.default
        let staticSource = sourceURL.appendingPathComponent("static", isDirectory: true)
        let staticRuntime = runtimeURL.appendingPathComponent("static", isDirectory: true)
        let scriptsSource = sourceURL.appendingPathComponent("scripts", isDirectory: true)
        let scriptsRuntime = runtimeURL.appendingPathComponent("scripts", isDirectory: true)
        let vendorSource = sourceURL.appendingPathComponent("vendor", isDirectory: true)
        let vendorRuntime = runtimeURL.appendingPathComponent("vendor", isDirectory: true)

        let rootEntries = try fileManager.contentsOfDirectory(
            at: sourceURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        )
        for entry in rootEntries where entry.pathExtension == "py" {
            let destination = runtimeURL.appendingPathComponent(entry.lastPathComponent)
            try copyItemReplacing(source: entry, destination: destination)
        }
        if fileManager.fileExists(atPath: staticSource.path) {
            try copyItemReplacing(source: staticSource, destination: staticRuntime)
        }
        if fileManager.fileExists(atPath: scriptsSource.path) {
            try copyItemReplacing(source: scriptsSource, destination: scriptsRuntime)
        }
        if fileManager.fileExists(atPath: vendorSource.path) {
            try copyItemReplacing(source: vendorSource, destination: vendorRuntime)
        }
    }

    private func shouldSkipRuntimeFingerprintPath(relativePath: String) -> Bool {
        let normalized = relativePath.replacingOccurrences(of: "\\", with: "/")
        let parts = normalized.split(separator: "/").map(String.init)
        if parts.contains("node_modules") { return true }
        if parts.contains("dist") { return true }
        if parts.contains("__pycache__") { return true }
        if parts.contains(".pytest_cache") { return true }
        if parts.contains(".mypy_cache") { return true }
        if parts.contains(".ruff_cache") { return true }
        if parts.contains(".cache") { return true }
        if parts.contains("coverage") { return true }
        if normalized.contains("/vendor/MiroFish/backend/.venv/") { return true }
        if normalized.hasPrefix("vendor/MiroFish/backend/.venv/") { return true }
        if normalized.contains("/vendor/MiroFish/frontend/node_modules/") { return true }
        if normalized.hasPrefix("vendor/MiroFish/frontend/node_modules/") { return true }
        return false
    }

    private func runtimeSourceStamp(for sourceURL: URL) throws -> String {
        let fileManager = FileManager.default
        let rootPythonFiles = try fileManager.contentsOfDirectory(
            at: sourceURL,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ).filter { $0.pathExtension == "py" }
        let interestingRoots = rootPythonFiles + [
            sourceURL.appendingPathComponent("static", isDirectory: true),
            sourceURL.appendingPathComponent("scripts", isDirectory: true),
            sourceURL.appendingPathComponent("vendor", isDirectory: true),
        ]
        var entries: [String] = []

        for root in interestingRoots {
            var isDirectory: ObjCBool = false
            guard fileManager.fileExists(atPath: root.path, isDirectory: &isDirectory) else { continue }
            if isDirectory.boolValue {
                guard let enumerator = fileManager.enumerator(
                    at: root,
                    includingPropertiesForKeys: [.contentModificationDateKey, .fileSizeKey, .isDirectoryKey],
                    options: [.skipsHiddenFiles]
                ) else { continue }
                for case let url as URL in enumerator {
                    let values = try? url.resourceValues(forKeys: [.isDirectoryKey, .contentModificationDateKey, .fileSizeKey])
                    if values?.isDirectory == true { continue }
                    let relativePath = url.path.replacingOccurrences(of: sourceURL.path + "/", with: "")
                    if shouldSkipRuntimeFingerprintPath(relativePath: relativePath) { continue }
                    let fileSize = values?.fileSize ?? 0
                    let timestamp = values?.contentModificationDate?.timeIntervalSince1970 ?? 0
                    entries.append("\(relativePath)|\(fileSize)|\(String(format: "%.6f", timestamp))")
                }
            } else {
                let values = try? root.resourceValues(forKeys: [.contentModificationDateKey, .fileSizeKey])
                let relativePath = root.lastPathComponent
                if shouldSkipRuntimeFingerprintPath(relativePath: relativePath) { continue }
                let fileSize = values?.fileSize ?? 0
                let timestamp = values?.contentModificationDate?.timeIntervalSince1970 ?? 0
                entries.append("\(relativePath)|\(fileSize)|\(String(format: "%.6f", timestamp))")
            }
        }

        entries.sort()
        let payload = entries.joined(separator: "\n")
        let digest = SHA256.hash(data: Data(payload.utf8))
        let digestHex = digest.map { String(format: "%02x", $0) }.joined()
        return "\(sourceURL.path)|\(entries.count)|\(digestHex)"
    }

    private func terminateManagedEmbeddedServices(runtimeSource: RuntimeAppSource) -> Bool {
        let managedPatterns = [
            Bundle.main.bundleURL.appendingPathComponent("Contents/Resources/app/server.py").path,
            runtimeSource.runtimeURL.appendingPathComponent("server.py").path,
        ]
        var terminatedAny = false
        for pattern in managedPatterns {
            guard !pattern.isEmpty else { continue }
            let task = Process()
            task.executableURL = URL(fileURLWithPath: "/usr/bin/pkill")
            task.arguments = ["-f", pattern]
            do {
                try task.run()
                task.waitUntilExit()
                terminatedAny = terminatedAny || task.terminationStatus == 0
            } catch {
                continue
            }
        }
        if terminatedAny {
            Thread.sleep(forTimeInterval: 0.35)
        }
        return terminatedAny
    }

    private func copyItemReplacing(source: URL, destination: URL) throws {
        let fileManager = FileManager.default
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        if !fileManager.fileExists(atPath: source.path) {
            return
        }
        try fileManager.copyItem(at: source, to: destination)
    }

    private func preferredPythonRuntime() -> PythonRuntime? {
        if let frameworksURL = Bundle.main.privateFrameworksURL {
            let versionsRoot = frameworksURL
                .appendingPathComponent("Python.framework/Versions", isDirectory: true)
            if let versionedRuntime = bundledPythonRuntime(in: versionsRoot) {
                return versionedRuntime
            }
        }

        let candidates = [
            ProcessInfo.processInfo.environment["PYTHON3_PATH"],
            "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3",
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3",
            "/usr/bin/python3"
        ].compactMap { $0 }

        for candidate in candidates {
            guard FileManager.default.isExecutableFile(atPath: candidate),
                  probePythonRuntime(executable: candidate, home: nil, version: nil) else {
                continue
            }
            return PythonRuntime(executable: candidate, home: nil, version: nil)
        }

        return nil
    }

    private func bundledPythonRuntime(in versionsRoot: URL) -> PythonRuntime? {
        guard let entries = try? FileManager.default.contentsOfDirectory(
            at: versionsRoot,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            return nil
        }

        let versionDirs = entries
            .filter { url in
                let name = url.lastPathComponent
                return name.range(of: #"^\d+\.\d+$"#, options: .regularExpression) != nil
            }
            .sorted { lhs, rhs in
                compareVersionStrings(lhs.lastPathComponent, rhs.lastPathComponent) == .orderedDescending
            }

        for versionDir in versionDirs {
            let version = versionDir.lastPathComponent
            let executable = versionDir.appendingPathComponent("bin/python3").path
            guard FileManager.default.isExecutableFile(atPath: executable),
                  probePythonRuntime(executable: executable, home: versionDir.path, version: version) else {
                continue
            }
            return PythonRuntime(executable: executable, home: versionDir.path, version: version)
        }

        return nil
    }

    private func compareVersionStrings(_ lhs: String, _ rhs: String) -> ComparisonResult {
        let left = lhs.split(separator: ".").compactMap { Int($0) }
        let right = rhs.split(separator: ".").compactMap { Int($0) }
        let count = max(left.count, right.count)
        for index in 0..<count {
            let leftValue = index < left.count ? left[index] : 0
            let rightValue = index < right.count ? right[index] : 0
            if leftValue == rightValue { continue }
            return leftValue < rightValue ? .orderedAscending : .orderedDescending
        }
        return .orderedSame
    }

    private func probePythonRuntime(executable: String, home: String?, version: String?) -> Bool {
        let probe = Process()
        probe.executableURL = URL(fileURLWithPath: executable)
        probe.arguments = ["-c", "import requests"]
        if let home {
            var env = ProcessInfo.processInfo.environment
            env["PYTHONHOME"] = home
            let pythonLibVersion = version ?? "3.12"
            env["PYTHONPATH"] = "\(home)/lib/python\(pythonLibVersion):\(home)/lib/python\(pythonLibVersion)/site-packages"
            probe.environment = env
        }

        do {
            try probe.run()
            probe.waitUntilExit()
            return probe.terminationStatus == 0
        } catch {
            return false
        }
    }

    private func detectExistingServiceURL(expectedRuntimeSyncStamp: String?) -> URL? {
        for port in healthPorts {
            guard let url = URL(string: "http://127.0.0.1:\(port)\(healthPath)") else {
                continue
            }
            if isHealthy(url: url, expectedRuntimeSyncStamp: expectedRuntimeSyncStamp) {
                return URL(string: "http://127.0.0.1:\(port)/")
            }
        }
        return nil
    }

    private func requestLooksHealthy(
        url: URL,
        timeout: TimeInterval,
        bodyMustContain fragment: String? = nil
    ) -> Bool {
        let semaphore = DispatchSemaphore(value: 0)
        var looksHealthy = false
        var request = URLRequest(url: url)
        request.timeoutInterval = timeout

        URLSession.shared.dataTask(with: request) { data, response, error in
            defer { semaphore.signal() }

            guard
                error == nil,
                let http = response as? HTTPURLResponse,
                http.statusCode == 200
            else {
                return
            }

            if let fragment {
                guard
                    let data,
                    let text = String(data: data, encoding: .utf8),
                    text.contains(fragment)
                else {
                    return
                }
            }

            looksHealthy = true
        }.resume()

        _ = semaphore.wait(timeout: .now() + timeout + 0.25)
        return looksHealthy
    }

    private func requestJSON(url: URL, timeout: TimeInterval) -> [String: Any]? {
        let semaphore = DispatchSemaphore(value: 0)
        var payload: [String: Any]?
        var request = URLRequest(url: url)
        request.timeoutInterval = timeout

        URLSession.shared.dataTask(with: request) { data, response, error in
            defer { semaphore.signal() }
            guard
                error == nil,
                let http = response as? HTTPURLResponse,
                http.statusCode == 200,
                let data,
                let json = try? JSONSerialization.jsonObject(with: data),
                let object = json as? [String: Any]
            else {
                return
            }
            payload = object
        }.resume()

        _ = semaphore.wait(timeout: .now() + timeout + 0.25)
        return payload
    }

    private func isHealthy(url: URL, expectedRuntimeSyncStamp: String?) -> Bool {
        guard requestLooksHealthy(url: url, timeout: 0.8, bodyMustContain: "\"service\": \"okx-local-app\"") else {
            return false
        }

        for compatibilityPath in compatibilityPaths {
            guard let compatibilityURL = URL(string: "http://127.0.0.1:\(url.port ?? 0)\(compatibilityPath)") else {
                return false
            }
            guard let payload = requestJSON(url: compatibilityURL, timeout: 1.5) else {
                return false
            }
            if compatibilityPath == "/api/local-config",
               let expectedRuntimeSyncStamp,
               !expectedRuntimeSyncStamp.isEmpty {
                let runtimeStamp = String(payload["runtimeSyncStamp"] as? String ?? "")
                if runtimeStamp != expectedRuntimeSyncStamp {
                    return false
                }
            }
        }

        return true
    }

    private func load(_ url: URL) {
        currentServiceURL = url
        var finalURL = url
        if var components = URLComponents(url: url, resolvingAgainstBaseURL: false) {
            var queryItems = components.queryItems ?? []
            queryItems.removeAll { item in
                item.name == "_runtimeStamp" || item.name == "_launchTs"
            }
            if !currentRuntimeSyncStamp.isEmpty {
                queryItems.append(URLQueryItem(name: "_runtimeStamp", value: currentRuntimeSyncStamp))
            }
            queryItems.append(URLQueryItem(name: "_launchTs", value: String(Int(Date().timeIntervalSince1970 * 1000))))
            components.queryItems = queryItems
            if let rebuilt = components.url {
                finalURL = rebuilt
            }
        }
        window.title = "\(appName) - \(url.host ?? "127.0.0.1"):\(url.port ?? 0)"
        var request = URLRequest(url: finalURL)
        request.cachePolicy = .reloadIgnoringLocalCacheData
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        webView.load(request)
    }

    private func showPlaceholder(title: String, detail: String) {
        let safeTitle = title
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
        let safeDetail = detail
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
            .replacingOccurrences(of: "\n", with: "<br>")

        let html = """
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8">
            <style>
              :root {
                color-scheme: dark;
              }
              body {
                margin: 0;
                background: radial-gradient(circle at top left, rgba(69,214,196,0.12), transparent 28%), radial-gradient(circle at top right, rgba(255,184,77,0.10), transparent 24%), #0b0f14;
                color: #e9eef5;
                font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", sans-serif;
                display: grid;
                place-items: center;
                min-height: 100vh;
              }
              .panel {
                width: min(680px, calc(100vw - 96px));
                border: 1px solid rgba(255,255,255,0.08);
                padding: 28px 30px;
                background: rgba(255,255,255,0.03);
              }
              .kicker {
                color: #45d6c4;
                text-transform: uppercase;
                letter-spacing: 0.18em;
                font-size: 11px;
                margin-bottom: 12px;
              }
              h1 {
                margin: 0 0 10px;
                font: 600 36px Georgia, serif;
              }
              p {
                margin: 0;
                color: #9aa6b4;
                line-height: 1.65;
              }
            </style>
          </head>
          <body>
            <div class="panel">
              <div class="kicker">OKX Local App</div>
              <h1>\(safeTitle)</h1>
              <p>\(safeDetail)</p>
            </div>
          </body>
        </html>
        """
        webView.loadHTMLString(html, baseURL: nil)
    }

    private func applicationSupportDirectory() -> URL {
        let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
        return base.appendingPathComponent("OKXLocalApp", isDirectory: true)
    }

    private func readProcessOutput() -> String {
        guard
            let logURL = serverLogURL,
            let data = try? Data(contentsOf: logURL),
            let text = String(data: data, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines),
            !text.isEmpty
        else {
            return ""
        }
        return text
    }

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        window.makeFirstResponder(webView)
    }
}

private extension String {
    func ifEmpty(_ fallback: String) -> String {
        isEmpty ? fallback : self
    }
}

let app = NSApplication.shared
let delegate = AppDelegate()
app.setActivationPolicy(.regular)
app.delegate = delegate
app.run()
