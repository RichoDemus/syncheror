package com.richodemus.syncheror.core

internal class Settings {
    val gcsProject = System.getProperty("syncheror.gcs.project") ?:
            throw IllegalArgumentException("Missing property GCS_PROJECT/syncheror.gcs.project")

    val gcsBucket = System.getProperty("syncheror.gcs.bucket") ?:
            throw IllegalArgumentException("Missing property GCS_PROJECT/syncheror.gcs.bucket")

    val kafkaServers = System.getProperty("syncheror.kafka.bootstrapServers") ?:
            throw IllegalArgumentException("Missing property GCS_PROJECT/syncheror.kafka.bootstrapServers")

    val kafkaTopic = System.getProperty("syncheror.kafka.topic") ?:
            throw IllegalArgumentException("Missing property GCS_PROJECT/syncheror.kafka.topic")

    override fun toString(): String {
        return "Settings(gcsProject='$gcsProject', gcsBucket='$gcsBucket', kafkaServers='$kafkaServers', kafkaTopic='$kafkaTopic')"
    }
}
