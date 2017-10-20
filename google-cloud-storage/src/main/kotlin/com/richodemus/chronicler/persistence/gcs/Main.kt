package com.richodemus.chronicler.persistence.gcs

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong


private val logger = LoggerFactory.getLogger("Main")!!

fun main(args: Array<String>) {
    val persister = GoogleCloudStoragePersistence()

    val topic = "events"

    val offset = topicToList(topic).lastOrNull()?.first ?: -1L
    logger.info("Topic offset is $offset")

    val producer = Producer(topic)
    val eventsFromGcs = persister.readEvents().asSequence().toList()
    logger.info("Got ${eventsFromGcs.size} events from GCS, time to send them to Kafka...")
    eventsFromGcs
            .filter { it.page!!.toOffset() < 30 } // use this to only produce the first x events
            .filter { it.page!!.toOffset() > offset }
            .forEach { producer.send(it.id, it) }

    producer.close()

    logger.info("Data sent to Kafka, time to verify...")
    val mapper = jacksonObjectMapper()
    val eventsFromKafka = topicToList(topic)
            .map { it.second }
            .map { mapper.readValue(it, EventDTO::class.java) }
            .map { it.toEvent() }

    eventsFromGcs.forEachIndexed { index, event ->
        if (index >= eventsFromKafka.size) {
            logger.warn("Event $event missing from Kafka")
        }
        else if (eventsFromKafka[index] != event) {
            logger.warn("Event mismatch: gcs: $event and kafka: ${eventsFromKafka[index]} doesn't match")
        }
    }

    logger.info("all events equal")

    logger.info("Store events in other bucket")
    val persistence = GoogleCloudStoragePersistence(bucket = "richo-test")
    val storedEvents = AtomicLong()
    val storageConsumer = Consumer(topic) { event ->
        // make sure it doesn't already exist
        persistence.persist(event.second)
        storedEvents.incrementAndGet()
    }

    while (storedEvents.get() < eventsFromKafka.size) {
        Thread.sleep(10L)
    }

    logger.info("Stored events in new bucket...")
    storageConsumer.stop()
}

private fun Long.toOffset() = this - 1
