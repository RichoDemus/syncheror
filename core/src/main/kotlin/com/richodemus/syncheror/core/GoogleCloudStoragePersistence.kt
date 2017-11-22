package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.StorageOptions
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.LongAdder
import java.util.function.Supplier
import javax.inject.Singleton


@Singleton
class GoogleCloudStoragePersistence {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val directory = "events/v1/"
    private val mapper = jacksonObjectMapper()
    private val settings = Settings()

    private val service = StorageOptions.newBuilder()
            .setProjectId(settings.gcsProject)
            .build()
            .service

    fun getNumberOfEvents(): Int {
        return service.list(settings.gcsBucket)
                .iterateAll().count()
    }

    fun readEvents(): Iterator<Event> {
        val threads = Runtime.getRuntime().availableProcessors()*100
        val executor = Executors.newFixedThreadPool(threads)
        var run = true
        val eventsStarted = LongAdder()
        val eventsDownloaded = LongAdder()
        var eventsDownloadedAtLastPrint = 0L
        try {
            logger.info("Preparing to download events from Google Cloud Storage, using {} threads...", threads)

            Thread(Runnable {
                while (run) {
                    if(eventsStarted.sum() > 0 || eventsDownloaded.sum() > 0) {
                        val downloaded = eventsDownloaded.sum()
                        val eventsPerSecond = (downloaded - eventsDownloadedAtLastPrint)
                        logger.info("Event downloads started: ${eventsStarted.sum()}, Events downloaded: ${eventsDownloaded.sum()}, Events per second: ${eventsPerSecond}")
                        eventsDownloadedAtLastPrint = downloaded
                    }
                    Thread.sleep(1_000L)
                }
            }).start()

            return service.list(settings.gcsBucket)
                    .iterateAll()
                    .filter { it.blobId.name.startsWith(directory) }
                    .map {
                        eventsStarted.increment()
                        CompletableFuture.supplyAsync(Supplier {
                            it.getContent()
                                    .let { String(it) }
                                    .toDto()
                                    .toEvent()
                        }, executor)
                    }
                    .map {
                        eventsDownloaded.increment()
                        it.get()
                    }
                    .sortedBy { it.page }
                    .iterator()

        } finally {
            run = false
            executor.shutdown()
        }
    }

    fun persist(event: Event) {
        val filename = "$directory${event.page.toString()}"
        val eventBytes = event.toDto().toJSONString().toByteArray()
        val blob = BlobId.of(settings.gcsBucket, filename)
        if (exists(blob)) {
            logger.info("File $filename already exists in GCS, skipping...")
            return
        }
        service.create(BlobInfo.newBuilder(blob).build(), eventBytes)
    }

    private fun exists(blob: BlobId): Boolean {
        return service.get(blob) != null
    }

    private fun Event.toDto(): EventDTO {
        val page = this.page ?: throw IllegalStateException("Can't save event without page")
        return EventDTO(this.id, this.type, page, this.data)
    }

    private fun String.toDto() = mapper.readValue(this, EventDTO::class.java)
}
