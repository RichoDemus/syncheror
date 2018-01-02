package com.richodemus.syncheror.core

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.StorageOptions
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.LongAdder
import java.util.function.Supplier
import javax.inject.Singleton
import kotlin.concurrent.thread


@Singleton
class GoogleCloudStoragePersistence {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val directory = "events/v2/"
    private val settings = Settings()

    private val service = StorageOptions.newBuilder()
            .setProjectId(settings.gcsProject)
            .build()
            .service

    internal fun readEvents(): Iterator<Event> {
        val threads = Runtime.getRuntime().availableProcessors() * 100
        val executor = Executors.newFixedThreadPool(threads)
        var run = true
        val eventsStarted = LongAdder()
        val eventsDownloaded = LongAdder()
        var eventsDownloadedAtLastPrint = 0L
        try {
            logger.info("Preparing to download events from Google Cloud Storage, using {} threads...", threads)

            thread(name = "print-download-progress") {
                while (run) {
                    if (eventsStarted.sum() > 0 || eventsDownloaded.sum() > 0) {
                        val downloaded = eventsDownloaded.sum()
                        val eventsPerSecond = (downloaded - eventsDownloadedAtLastPrint)
                        logger.info("Event downloads started: ${eventsStarted.sum()}, Events downloaded: ${eventsDownloaded.sum()}, Events per second: $eventsPerSecond")
                        eventsDownloadedAtLastPrint = downloaded
                    }
                    Thread.sleep(1_000L)
                }
            }

            return service.list(settings.gcsBucket)
                    .iterateAll()
                    .filter { it.blobId.name.startsWith(directory) }
                    .map {
                        eventsStarted.increment()
                        CompletableFuture.supplyAsync(Supplier {
                            val offset = it.blobId.name.split("/")[2].toLong()
                            val data = it.getContent().let { String(it) }
                            val key = data.substringBefore(",")
                            val event = data.substringAfter(",")
                            Event(Offset(offset), Key(key), Data(event))
                        }, executor)
                    }
                    .map {
                        eventsDownloaded.increment()
                        it.get()
                    }
                    .sortedBy { it.offset.value }
                    .iterator()

        } finally {
            run = false
            executor.shutdown()
        }
    }

    internal fun persist(event: Event) {
        val filename = "$directory${event.offset.value}"
        val data = "${event.key.value},${event.data.value}"
        val eventBytes = data.toByteArray()
        val blob = BlobId.of(settings.gcsBucket, filename)
        if (exists(blob)) {
            logger.info("File $filename already exists in GCS, skipping...")
            return
        }
        service.create(BlobInfo.newBuilder(blob).build(), eventBytes)
//        logger.info("Would've saved $filename: ${String(eventBytes)}")
    }

    private fun exists(blob: BlobId) = service.get(blob) != null
}
