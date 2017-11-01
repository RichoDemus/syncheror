package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.common.util.concurrent.ThreadFactoryBuilder
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
                        logger.info("$gcsEvent missing from Kafka, adding it")
                        producer.send(gcsEvent!!.id, gcsEvent)
                    }
                    if (gcsEvent == null) {
                        logger.info("$kafkaEvent missing from GCS, adding it, ish...")
                        persister.persist(kafkaEvent)
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
