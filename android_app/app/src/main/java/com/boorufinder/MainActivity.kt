package com.boorufinder.app

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast
import androidx.activity.addCallback
import androidx.appcompat.app.AppCompatActivity
import com.chaquo.python.Python
import com.chaquo.python.android.AndroidPlatform
import java.net.HttpURLConnection
import java.net.URL
import kotlin.concurrent.thread

class MainActivity : AppCompatActivity() {
    private lateinit var webView: WebView
    private var loadAttempt = 0
    private var restorePending = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        webView = findViewById(R.id.webView)
        configureWebView()
        startPythonBackend()
        restorePending = savedInstanceState != null
        waitAndLoadUi(savedInstanceState)

        onBackPressedDispatcher.addCallback(this) {
            if (webView.canGoBack()) {
                webView.goBack()
            } else {
                finish()
            }
        }
    }

    @Suppress("SetJavaScriptEnabled")
    private fun configureWebView() {
        webView.settings.javaScriptEnabled = true
        webView.settings.domStorageEnabled = true
        webView.settings.mediaPlaybackRequiresUserGesture = false
        webView.settings.allowContentAccess = true
        webView.settings.allowFileAccess = true
        webView.settings.loadsImagesAutomatically = true

        webView.webChromeClient = WebChromeClient()
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                val url = request.url.toString()
                if (url.startsWith(BASE_URL)) {
                    return false
                }
                startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
                return true
            }

            override fun onReceivedError(
                view: WebView,
                request: WebResourceRequest,
                error: WebResourceError
            ) {
                super.onReceivedError(view, request, error)
                if (request.isForMainFrame && request.url.toString().startsWith(BASE_URL)) {
                    waitAndLoadUi()
                }
            }
        }
    }

    private fun startPythonBackend() {
        thread(name = "booru-python-start", isDaemon = true) {
            try {
                if (!Python.isStarted()) {
                    Python.start(AndroidPlatform(this))
                }
                Python.getInstance().getModule("android_entry").callAttr("start_server", PORT)
            } catch (exc: Exception) {
                runOnUiThread {
                    Toast.makeText(
                        this,
                        "Backend start failed: ${exc.message}",
                        Toast.LENGTH_LONG
                    ).show()
                }
            }
        }
    }

    private fun waitAndLoadUi(savedInstanceState: Bundle? = null) {
        thread(name = "booru-ui-load", isDaemon = true) {
            val reachable = waitForServer()
            runOnUiThread {
                if (!reachable && loadAttempt == 0) {
                    Toast.makeText(this, "Server warmup is taking longer than usual", Toast.LENGTH_SHORT).show()
                }
                if (restorePending && savedInstanceState != null) {
                    webView.restoreState(savedInstanceState)
                    restorePending = false
                    return@runOnUiThread
                }
                webView.loadUrl("$BASE_URL/?android=1&attempt=${loadAttempt++}")
            }
        }
    }

    private fun waitForServer(): Boolean {
        repeat(60) {
            if (isServerReady()) {
                return true
            }
            Thread.sleep(250)
        }
        return false
    }

    private fun isServerReady(): Boolean {
        return try {
            val connection = (URL(BASE_URL).openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 1200
                readTimeout = 1200
            }
            connection.responseCode in 200..399
        } catch (_: Exception) {
            false
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        webView.saveState(outState)
    }

    override fun onDestroy() {
        webView.stopLoading()
        webView.destroy()
        super.onDestroy()
    }

    companion object {
        private const val PORT = 8765
        private const val BASE_URL = "http://127.0.0.1:8765"
    }
}
