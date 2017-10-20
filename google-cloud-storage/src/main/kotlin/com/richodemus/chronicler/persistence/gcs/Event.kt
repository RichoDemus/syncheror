package com.richodemus.chronicler.persistence.gcs

data class Event(val id: String, val type: String, val page: Long?, val data: String)
