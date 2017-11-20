package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.UUID

/**
 * Helper class to consume messages from a topic
 */
internal class Consumer(kafkaServers: String, topic: String, private val messageListener: (Long, Event) -> Unit) {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val mapper = jacksonObjectMapper()
    private val consumer: KafkaConsumer<String, String>
    private var running = true

    init {
        val props = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "SyncherorConsumer-${UUID.randomUUID()}")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        consumer = KafkaConsumer(props)

        consumer.subscribe(listOf(topic))

        Thread {
            try {
                while (running) {
                    val records = consumer.poll(Long.MAX_VALUE)

                    if (records.isEmpty) {
                        logger.debug("No messages...")
                        continue
                    }

                    records.map {
                        logger.debug("Received: {}: {}", it.key(), it.value())
                        Pair(it.offset(), it.value())
                    }
                            .map { Pair(it.first, mapper.readValue(it.second, EventDTO::class.java)) }
                            .map { Pair(it.first, it.second.toEvent()) }
                            .forEach { messageListener(it.first, it.second) }
                }
            } catch (e: WakeupException) {
                logger.info("consumer.wakeup() called, Kafka consumer shutting down")
            } finally {
                consumer.close()
            }
        }.start()
        Runtime.getRuntime().addShutdownHook(Thread { stop() })
    }

    fun stop() {
        running = false
        consumer.wakeup()
    }
}
