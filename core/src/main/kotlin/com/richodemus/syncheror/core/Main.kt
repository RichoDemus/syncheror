package com.richodemus.syncheror.core

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.DAYS

private val logger = LoggerFactory.getLogger("Main")!!

/**
 * ./kafka_2.11-1.0.0/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic events --partitions 1 --replication-factor 1 --config retention.ms=-1
 */
fun main(args: Array<String>) {
    val settings = Settings()
    println(settings.toString())

    if (settings.newImplementation) {
        newImpl()
        return
    }

    val executor = ScheduledThreadPoolExecutor(1, ThreadFactoryBuilder()
            .setNameFormat("sync-thread-%s")
            .setDaemon(false)
            .build())
    Runtime.getRuntime().addShutdownHook(Thread(Runnable { executor.shutdown() }))

    val bidirectionalSyncer = BidirectionalSyncer(settings.syncDirection) { executor.shutdown() }

    executor.scheduleAtFixedRate(bidirectionalSyncer, 0L, 1, DAYS)
}

fun newImpl() {
    // initially fill kafka with events from gcs
    val bidirectionalSyncer = BidirectionalSyncer(SyncDirection.GCS_TO_KAFKA) { }
    val page = bidirectionalSyncer.syncOnce() - 10
    logger.info("Page: $page")

    val settings = Settings()
    val googleCloudStoragePersistence = GoogleCloudStoragePersistence()
    Consumer(settings.kafkaServers, settings.kafkaTopic) { offset, event ->
        val eventWithCorrectPage = event.copy(page = offset.inc())
        if (offset > page) {
            logger.info("Got message. offset: $offset, event: $eventWithCorrectPage")
            googleCloudStoragePersistence.persist(eventWithCorrectPage)
        } else
            if (offset > 16595) logger.info("Skipping offset $offset")
    }
}
