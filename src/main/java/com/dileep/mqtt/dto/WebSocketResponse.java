package com.dileep.mqtt.dto;

public record WebSocketResponse(
        String type,
        String topic,
        Object payload
) {
}