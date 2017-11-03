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

    val syncDirection = SyncDirection.valueOf(System.getProperty("syncheror.syncDirection")?.toUpperCase() ?:
            throw IllegalArgumentException("Missing property SYNC_DIRECTION/syncheror.syncDirection"))

    override fun toString(): String {
        return "Settings(gcsProject='$gcsProject', gcsBucket='$gcsBucket', kafkaServers='$kafkaServers', kafkaTopic='$kafkaTopic', syncDirection=$syncDirection)"
    }
}

internal enum class SyncDirection {
    GCS_TO_KAFKA,
    KAFKA_TO_GCS,
    BIDIRECTIONAL
}
