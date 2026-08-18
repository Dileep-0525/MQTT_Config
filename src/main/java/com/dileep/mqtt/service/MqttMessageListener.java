package com.dileep.mqtt.service;

public interface MqttMessageListener {

	void onMessage(String topic, String payload);
	
}
