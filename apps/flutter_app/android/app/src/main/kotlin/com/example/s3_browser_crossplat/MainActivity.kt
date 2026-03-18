package com.example.s3_browser_crossplat

import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel

class MainActivity : FlutterActivity() {
    private val channelName = "s3_browser_crossplat/android_engine"

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, channelName)
            .setMethodCallHandler(::handleMethodCall)
    }

    private fun handleMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "listEngines" -> result.success(
                listOf(
                    mapOf(
                        "id" to "go",
                        "label" to "Go (Android)",
                        "language" to "go",
                        "platforms" to listOf("android"),
                        "availability" to "beta",
                        "notes" to "Android native adapter entry point.",
                    ),
                    mapOf(
                        "id" to "rust",
                        "label" to "Rust (Android)",
                        "language" to "rust",
                        "platforms" to listOf("android"),
                        "availability" to "beta",
                        "notes" to "Android native adapter entry point.",
                    ),
                ),
            )

            "dispatch" -> {
                val args = call.arguments as? Map<*, *> ?: emptyMap<String, Any?>()
                val engineId = args["engineId"] as? String ?: "android"
                val method = args["method"] as? String ?: "unknown"
                result.success(dispatch(engineId, method))
            }

            else -> result.notImplemented()
        }
    }

    private fun dispatch(engineId: String, method: String): Map<String, Any?> {
        return when (method) {
            "health" -> mapOf(
                "engine" to engineId,
                "version" to "2.0.8",
                "available" to true,
                "methods" to listOf("health", "getCapabilities"),
                "adapter" to "android-platform-channel",
            )

            "getCapabilities" -> mapOf(
                "items" to listOf(
                    mapOf(
                        "key" to "android.adapter",
                        "label" to "Android native engine adapter",
                        "state" to "supported",
                    ),
                    mapOf(
                        "key" to "android.rotation",
                        "label" to "Rotation-safe activity recreation",
                        "state" to "supported",
                    ),
                ),
            )

            else -> mapOf(
                "error" to mapOf(
                    "code" to "unsupported_feature",
                    "message" to "Android adapter method $method is not implemented yet.",
                ),
            )
        }
    }
}
