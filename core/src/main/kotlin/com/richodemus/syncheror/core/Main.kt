package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.richodemus.syncheror.core.SyncDirection.BIDIRECTIONAL
import com.richodemus.syncheror.core.SyncDirection.GCS_TO_KAFKA
import com.richodemus.syncheror.core.SyncDirection.KAFKA_TO_GCS
import org.slf4j.LoggerFactory
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.DAYS


private val logger = LoggerFactory.getLogger("Main")!!
private val mapper = jacksonObjectMapper()

fun main(args: Array<String>) {
    val settings = Settings()
    println(settings.toString())

    val executor = ScheduledThreadPoolExecutor(1, ThreadFactoryBuilder()
            .setNameFormat("sync-thread-%s")
            .setDaemon(false)
            .build())
    Runtime.getRuntime().addShutdownHook(Thread(Runnable { executor.shutdown() }))
    val persister = GoogleCloudStoragePersistence()

    settings.kafkaTopic

    val runnable = Runnable {
        try {
            logger.info("Time to sync!")
            val eventsInKafka = topicToList()
                    .map { it.second }
                    .map { mapper.readValue(it, EventDTO::class.java) }
                    .map { it.toEvent() }
            val eventsInGcs = persister.readEvents().asSequence().toList()

            logger.info("Got ${eventsInKafka.size} events from Kafka and ${eventsInGcs.size} events from GCS")
            Producer().use { producer ->
                IntRange(0, Math.max(eventsInKafka.size, eventsInGcs.size) - 1).forEach { i ->
                    val kafkaEvent = eventsInKafka.getOrNull(i)
                    val gcsEvent = eventsInGcs.getOrNull(i)

                    if (kafkaEvent == null) {
                        if (settings.syncDirection == GCS_TO_KAFKA || settings.syncDirection == BIDIRECTIONAL) {
                            logger.info("$gcsEvent missing from Kafka, adding it")
                            producer.send(gcsEvent!!.id, gcsEvent)
                        } else {
                            logger.info("$gcsEvent missing from Kafka")
                        }
                    }

                    if (gcsEvent == null) {
                        val eventWithCorrectPage = kafkaEvent!!.copy(page = i.toLong().inc())
                        if (settings.syncDirection == KAFKA_TO_GCS || settings.syncDirection == BIDIRECTIONAL) {
                            logger.info("$eventWithCorrectPage missing from GCS, adding it")
                            persister.persist(eventWithCorrectPage)
                        } else {
                            logger.info("$eventWithCorrectPage missing from GCS")
                        }
                    }

                    if (gcsEvent != null && kafkaEvent != null) {
                        if (kafkaEvent != gcsEvent) {
                            logger.warn("Event mismatch: $kafkaEvent, $gcsEvent")
                            executor.shutdown()
                            return@Runnable
                        }
                    }
                }
            }
            logger.info("Sync done")
        } catch (e: Exception) {
            logger.error("Sync run failed", e)
        }
    }

    executor.scheduleAtFixedRate(runnable, 0L, 1, DAYS)
}
