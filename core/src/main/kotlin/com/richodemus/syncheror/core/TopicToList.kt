package com.richodemus.syncheror.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger("topicToList")!!

internal fun topicToList(): List<Event> {
    val settings = Settings()
    val props = Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaServers)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "topic-to-list-${UUID.randomUUID()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = KafkaConsumer<String, String>(props)

    consumer.subscribe(listOf(settings.kafkaTopic))

    val events = mutableListOf<Event>()
    var numberOfEmptyPolls = 0
    if (logger.isDebugEnabled) {
        Thread({
            while (numberOfEmptyPolls < 5) {
                logger.debug("Read ${events.size} messages with $numberOfEmptyPolls empty polls")
                Thread.sleep(1_000L)
            }
        }).start()
    }
    logger.info("Downloading all events from kafka...")
    while (numberOfEmptyPolls < 5) {
        val records = consumer.poll(1000)

        if (records.isEmpty) {
            numberOfEmptyPolls++
        } else {
            records.map { Event(Offset(it.offset()), Key(it.key()), Data(it.value())) }.forEach { events.add(it) }
            numberOfEmptyPolls = 0
        }
    }
    logger.info("Got ${events.size} from kafka")
    consumer.close()
    return events
}

fun main(args: Array<String>) {
    val list = topicToList()
    logger.info("Done, got ${list.size} events")
}