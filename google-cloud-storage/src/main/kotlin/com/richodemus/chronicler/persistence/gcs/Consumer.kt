package com.richodemus.chronicler.persistence.gcs

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.UUID

internal class Consumer(topic: String, private val messageListener: (Pair<Long, Event>) -> Unit) {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val mapper = jacksonObjectMapper()
    private val consumer: KafkaConsumer<String, String>
    private var running = true

    init {
        val props = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Consumer-${UUID.randomUUID()}")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        consumer = KafkaConsumer(props)

        consumer.subscribe(listOf(topic))

        Thread {
            while (running) {
                val records = consumer.poll(5000)

                if (records.isEmpty) {
                    logger.info("No messages...")
                    continue
                }

                records.map {
                    logger.debug("Received event, key: {}, offset: {}, data: {}", it.key(), it.offset(), it.value())
                    Pair(it.offset(), it.value())
                }
                        .map { Pair(it.first, mapper.readValue<Event>(it.second)) }
                        .forEach { messageListener.invoke(it) }
            }
        }.start()
    }

    fun stop() {
        running = false
    }
}
