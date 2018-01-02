package com.richodemus.syncheror.core

import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("Main")!!

/**
 * ./kafka_2.11-1.0.0/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic events --partitions 1 --replication-factor 1 --config retention.ms=-1
 */
fun main(args: Array<String>) {
    val settings = Settings()
    println(settings.toString())

    val gcsToKafkaSyncer = GcsToKafkaSyncer()
    val offsetOfLatestGcsMessage = gcsToKafkaSyncer.syncGcsEventsToKafka()
    logger.info("Offset of latest GCS message: $offsetOfLatestGcsMessage")
    val googleCloudStoragePersistence = GoogleCloudStoragePersistence()
    Consumer(settings.kafkaServers, settings.kafkaTopic) { event ->
        if (event.offset.value > offsetOfLatestGcsMessage - 10) {
            logger.info("Saving to gcs: $event")
            googleCloudStoragePersistence.persist(event)
        }
    }
}
