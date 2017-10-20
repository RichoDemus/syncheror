package com.richodemus.syncheror.core

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

data class EventDTO
@JsonCreator constructor(@JsonProperty("id") val id: String,
                         @JsonProperty("type") val type: String,
                         @JsonProperty("page") val page: Long,
                         @JsonProperty("data") val data: String)

fun EventDTO.toEvent() = Event(this.id, this.type, this.page, this.data)

private val mapper = ObjectMapper().apply { registerModule(KotlinModule()) }
fun EventDTO.toJSONString() = mapper.writeValueAsString(this)
