package com.dileep.mqtt.dto;

public record WebSocketRequest( String action, String token, String topic ,Object payload) {

}
