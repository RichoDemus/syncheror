package com.richodemus.syncheror.core

import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.DAYS

fun main(args: Array<String>) {
    val settings = Settings()
    println(settings.toString())

    val executor = ScheduledThreadPoolExecutor(1, ThreadFactoryBuilder()
            .setNameFormat("sync-thread-%s")
            .setDaemon(false)
            .build())
    Runtime.getRuntime().addShutdownHook(Thread(Runnable { executor.shutdown() }))

    val bidirectionalSyncer = BidirectionalSyncer(settings.syncDirection) { executor.shutdown() }

    executor.scheduleAtFixedRate(bidirectionalSyncer, 0L, 1, DAYS)
}
