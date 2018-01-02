package com.richodemus.syncheror.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Helper class to consume messages from a topic
 */
internal class Consumer(kafkaServers: String, topic: String, private val messageListener: (Event) -> Unit) {
    private val logger = LoggerFactory.getLogger(javaClass.name)
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
                        messageListener(Event(Offset(it.offset()), Key(it.key()), Data(it.value())))
                    }
                }
            } catch (e: WakeupException) {
                logger.info("consumer.wakeup() called, Kafka consumer shutting down")
            } finally {
                consumer.close()
            }
        }.start()
        Runtime.getRuntime().addShutdownHook(Thread { stop() })
    }

    private fun stop() {
        running = false
        consumer.wakeup()
    }
}
