package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory

internal class BidirectionalSyncer(private val shutdown: () -> Unit) : Runnable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = jacksonObjectMapper()
    private val persister = GoogleCloudStoragePersistence()
    private val settings = Settings()

    override fun run() {
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
                        if (settings.syncDirection == SyncDirection.GCS_TO_KAFKA || settings.syncDirection == SyncDirection.BIDIRECTIONAL) {
                            logger.info("$gcsEvent missing from Kafka, adding it")
                            producer.send(gcsEvent!!.id, gcsEvent)
                        } else {
                            logger.info("$gcsEvent missing from Kafka")
                        }
                    }

                    if (gcsEvent == null) {
                        val eventWithCorrectPage = kafkaEvent!!.copy(page = i.toLong().inc())
                        if (settings.syncDirection == SyncDirection.KAFKA_TO_GCS || settings.syncDirection == SyncDirection.BIDIRECTIONAL) {
                            logger.info("$eventWithCorrectPage missing from GCS, adding it")
                            persister.persist(eventWithCorrectPage)
                        } else {
                            logger.info("$eventWithCorrectPage missing from GCS")
                        }
                    }

                    if (gcsEvent != null && kafkaEvent != null) {
                        val eventWithCorrectPage = kafkaEvent.copy(page = i.toLong().inc())
                        if (eventWithCorrectPage != gcsEvent) {
                            logger.warn("Event mismatch: $eventWithCorrectPage, $gcsEvent")
                            shutdown()
                            return@run
                        }
                    }
                }
            }
            logger.info("Sync done")
        } catch (e: Exception) {
            logger.error("Sync run failed", e)
        }
    }
}