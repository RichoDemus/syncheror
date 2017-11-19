package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory

internal class BidirectionalSyncer(private val syncDirection: SyncDirection, private val shutdown: () -> Unit) : Runnable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val mapper = jacksonObjectMapper()
    private val persister = GoogleCloudStoragePersistence()

    override fun run() {
        try {
            syncOnce()
        } catch (e: Exception) {
            shutdown()
            throw e
        }
    }

    private fun syncOnce() {
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
                        if (syncDirection == SyncDirection.GCS_TO_KAFKA || syncDirection == SyncDirection.BIDIRECTIONAL) {
                            logger.info("$gcsEvent missing from Kafka, adding it")
                            producer.send(gcsEvent!!.id, gcsEvent)
                        } else {
                            logger.info("$gcsEvent missing from Kafka")
                        }
                    }

                    if (gcsEvent == null) {
                        val eventWithCorrectPage = kafkaEvent!!.copy(page = i.toLong().inc())
                        if (syncDirection == SyncDirection.KAFKA_TO_GCS || syncDirection == SyncDirection.BIDIRECTIONAL) {
                            logger.info("$eventWithCorrectPage missing from GCS, adding it")
                            persister.persist(eventWithCorrectPage)
                        } else {
                            logger.info("$eventWithCorrectPage missing from GCS")
                        }
                    }

                    if (gcsEvent != null && kafkaEvent != null) {
                        val eventWithCorrectPage = kafkaEvent.copy(page = i.toLong().inc())
                        if (eventWithCorrectPage != gcsEvent) {
                            val msg = "Event mismatch: $eventWithCorrectPage, $gcsEvent"
                            logger.warn(msg)
                            throw IllegalStateException(msg)
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