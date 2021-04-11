package me.vitaly.etl.model

data class SessionLog(
    val device_id: String,
    val timestamp: Long,
    val event: String,
    val product: String,
    val session_id: String?,
)
