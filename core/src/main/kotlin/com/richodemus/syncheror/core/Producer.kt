package com.richodemus.syncheror.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.Properties

internal class Producer : Closeable {
    private val mapper = jacksonObjectMapper()
    private val producer: KafkaProducer<String, String>
    private val topic = Settings().kafkaTopic

    init {
        val props = Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        producer = KafkaProducer(props)
    }

    fun send(key: String, message: Event): RecordMetadata? {
        val json = mapper.writeValueAsString(message)
        val record: ProducerRecord<String, String> = ProducerRecord(topic, key, json)

        return producer.send(record).get()
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}
