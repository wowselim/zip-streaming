package co.selim.zip_streaming

import io.vertx.core.buffer.Buffer
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.toReceiveChannel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class MainVerticle : CoroutineVerticle() {

  private val files = listOf("hello.txt", "bye.txt")

  override suspend fun start() {
    val router = Router.router(vertx)

    router.get("/zip")
      .coroutineHandler { ctx ->
        ctx.response().isChunked = true
        ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/zip")
        ctx.response().putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"archive.zip\"")

        val byteArrayOutputStream = ByteArrayOutputStream()
        ZipOutputStream(byteArrayOutputStream).use { zip ->
          files.forEach { filename ->
            vertx.fileSystem().open(filename, OpenOptions())
              .await()
              .use { channel ->
                awaitBlocking { zip.putNextEntry(ZipEntry(filename)) }
                for (chunk in channel) {
                  awaitBlocking {
                    zip.write(chunk.bytes)
                    zip.flush()
                  }
                  ctx.response().write(byteArrayOutputStream.toBuffer()).await()
                  byteArrayOutputStream.reset()
                }
              }
            awaitBlocking {
              zip.closeEntry()
              zip.flush()
            }
            ctx.response().write(byteArrayOutputStream.toBuffer()).await()
            byteArrayOutputStream.reset()
          }
        }

        ctx.end(byteArrayOutputStream.toBuffer()).await()
      }

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(8080)
      .await()
  }

  private suspend fun AsyncFile.use(block: suspend (ReceiveChannel<Buffer>) -> Unit) {
    try {
      block(toReceiveChannel(vertx))
    } finally {
      close().await()
    }
  }

  private fun ByteArrayOutputStream.toBuffer(): Buffer {
    return Buffer.buffer(toByteArray())
  }

  private fun Route.coroutineHandler(
    block: suspend (RoutingContext) -> Unit
  ): Route = handler { ctx ->
    launch {
      try {
        block(ctx)
      } catch (t: Throwable) {
        ctx.fail(t)
      }
    }
  }
}
