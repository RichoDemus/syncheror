package com.richodemus.syncheror.core

import org.slf4j.LoggerFactory

internal class GcsToKafkaSyncer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val googleCloudStoragePersistence = GoogleCloudStoragePersistence()

    internal fun syncGcsEventsToKafka(): Int {
        try {
            logger.info("Time to sync!")
            // todo use fancy coroutines to do these two in parallel! :D
            val eventsInKafka = topicToList()
            val eventsInGcs = googleCloudStoragePersistence.readEvents().asSequence().toList()

            logger.info("Got ${eventsInKafka.size} events from Kafka and ${eventsInGcs.size} events from GCS")
            Producer().use { producer ->
                IntRange(0, Math.max(eventsInKafka.size, eventsInGcs.size) - 1).forEach { i ->
                    val kafkaEvent = eventsInKafka.getOrNull(i)
                    val gcsEvent = eventsInGcs.getOrNull(i)

                    // event is in gcs but not in kafka
                    if (kafkaEvent == null) {
                        logger.info("Adding missing event to kafka: $gcsEvent")
                        producer.send(gcsEvent!!)
                    }

                    // event is in kafka but not in gcs
                    if (gcsEvent == null) {
                        logger.info("${kafkaEvent!!} missing from GCS")
                    }

                    // event is in both kafka and gcs
                    if (gcsEvent != null && kafkaEvent != null) {
                        if (gcsEvent != kafkaEvent) {
                            val msg = "Event mismatch: $kafkaEvent, $gcsEvent"
                            logger.warn(msg)
                            throw IllegalStateException(msg)
                        }
                    }
                }
            }
            logger.info("Sync done")
            return eventsInGcs.size
        } catch (e: Exception) {
            throw IllegalStateException("Sync run failed", e)
        }
    }
}