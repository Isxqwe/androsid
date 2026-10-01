package com.androsid

import android.net.LocalServerSocket
import android.net.LocalSocket
import android.system.Os
import android.system.OsConstants
import android.util.Log
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import kotlin.concurrent.thread

class StreamServer(
    private val socketName: String,
    private val idleTimeoutMs: Long = 1000L,
    private val onCommandReceived: ((String) -> Unit)? = null
) {

    companion object {
        private const val TAG = "StreamServer"
    }

    private class Client(val socket: LocalSocket) {
        val out = BufferedOutputStream(socket.outputStream, 64 * 1024)
        @Volatile var lastSeenAt: Long = System.currentTimeMillis()
    }

    @Volatile private var client: Client? = null
    private var server: LocalServerSocket? = null
    @Volatile private var running = false

    fun start() {
        if (running) return
        running = true
        thread(name = "androsid-accept", isDaemon = true) {
            while (running) {
                try {
                    LocalServerSocket(socketName).use { srv ->
                        server = srv
                        Log.i(TAG, "listening on abstract socket '$socketName'")
                        val sock = srv.accept()
                        if (!running) {
                            sock.close()
                            return@use
                        }

                        val currentClient = Client(sock)
                        this.client = currentClient
                        Log.i(TAG, "client connected")

                        thread(name = "androsid-reader", isDaemon = true) {
                            try {
                                val reader = BufferedReader(InputStreamReader(sock.inputStream, Charsets.UTF_8))
                                while (running) {
                                    val line = reader.readLine() ?: break
                                    if (line.isNotBlank()) {
                                        currentClient.lastSeenAt = System.currentTimeMillis()
                                        onCommandReceived?.invoke(line)
                                    }
                                }
                            } catch (e: Exception) {
                                if (running) {
                                    Log.i(TAG, "client read ended: ${e.message}")
                                }
                            } finally {
                                dropClient(currentClient)
                            }
                        }

                        while (running && client != null) Thread.sleep(idleTimeoutMs)
                    }
                } catch (e: Exception) {
                    if (running) {
                        Log.e(TAG, "accept loop died", e)
                        Thread.sleep(idleTimeoutMs)
                    }
                }
            }
        }

        thread(name = "androsid-watchdog", isDaemon = true) {
            while (running) {
                Thread.sleep(idleTimeoutMs)
                val c = client ?: continue

                val idleMs = System.currentTimeMillis() - c.lastSeenAt
                if (idleMs > idleTimeoutMs) {
                    Log.w(TAG, "client timed out after ${idleMs}ms idle; disconnecting")
                    dropClient(c)
                }
            }
        }
    }

    fun stop() {
        running = false
        server?.let { srv ->
            // close() alone doesn't wake a blocked accept(), which keeps the name bound
            try { Os.shutdown(srv.fileDescriptor, OsConstants.SHUT_RDWR) } catch (_: Exception) {}
            try { srv.close() } catch (_: Exception) {}
        }
        server = null
        client?.let { try { it.socket.close() } catch (_: Exception) {} }
        client = null
    }

    fun isConnected(): Boolean = client != null

    fun broadcast(json: String) =
        broadcastLine((json + "\n").toByteArray(Charsets.UTF_8))

    fun broadcastLine(line: ByteArray) {
        val c = client ?: return
        writeTo(c, line)
    }

    private fun writeTo(c: Client, line: ByteArray) {
        try {
            synchronized(c) {
                c.out.write(line)
                c.out.flush()
            }
            c.lastSeenAt = System.currentTimeMillis()
        } catch (e: Exception) {
            Log.i(TAG, "client dropped: ${e.message}")
            dropClient(c)
        }
    }

    private fun dropClient(c: Client) {
        synchronized(this) {
            if (client === c) client = null
        }
        try { c.socket.close() } catch (_: Exception) {}
    }
}
