package me.vitaly.etl.model

data class RawLog(
    val device_id: String,
    val timestamp: Long,
    val event: String,
    val product: String
) {
    fun toSessionLog(sessionId: String? = null) = SessionLog(
        device_id = device_id,
        timestamp = timestamp,
        event = event,
        product = product,
        session_id = sessionId
    )
}
