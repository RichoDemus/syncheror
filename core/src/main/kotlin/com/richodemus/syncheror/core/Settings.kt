package com.richodemus.syncheror.core

internal class Settings {
    val gcsProject = System.getProperty("syncheror.gcs.project").emptyToNull() ?:
            throw IllegalArgumentException("Missing property GCS_PROJECT/syncheror.gcs.project")

    val gcsBucket = System.getProperty("syncheror.gcs.bucket").emptyToNull() ?:
            throw IllegalArgumentException("Missing property GCS_BUCKET/syncheror.gcs.bucket")

    val kafkaServers = System.getProperty("syncheror.kafka.bootstrapServers").emptyToNull() ?:
            throw IllegalArgumentException("Missing property KAFKA_SERVERS/syncheror.kafka.bootstrapServers")

    val kafkaTopic = System.getProperty("syncheror.kafka.topic").emptyToNull() ?:
            throw IllegalArgumentException("Missing property KAFKA_TOPIC/syncheror.kafka.topic")

    override fun toString() =
            "Settings(gcsProject='$gcsProject', gcsBucket='$gcsBucket', kafkaServers='$kafkaServers', kafkaTopic='$kafkaTopic')"
}

private fun String.emptyToNull(): String? {
    if (this.isBlank())
        return null
    return this
}
