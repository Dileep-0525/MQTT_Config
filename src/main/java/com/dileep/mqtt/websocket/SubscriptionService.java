package com.dileep.mqtt.websocket;
import java.util.Set;

import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import com.dileep.mqtt.service.MqttGatewayService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class SubscriptionService {

	private final SubscriptionRegistry registry;

	private final MqttGatewayService mqttGatewayService;

	/**
	 * Subscribe websocket session to topic.
	 */
	public void subscribe(WebSocketSession session, String topic) {

		boolean firstSubscriber = registry.subscribe(session, topic);

		if (firstSubscriber) {

			mqttGatewayService.subscribe(topic);

		}
	}

	/**
	 * Unsubscribe websocket session from topic.
	 */
	public void unsubscribe(WebSocketSession session, String topic) {

		boolean lastSubscriber = registry.unsubscribe(session, topic);

		if (lastSubscriber) {

			mqttGatewayService.unsubscribe(topic);

		}
	}

	/**
	 * Remove complete websocket session. Called when browser disconnects.
	 */
	public void removeSession(WebSocketSession session) {

		Set<String> topics = registry.removeSession(session);

		for (String topic : topics) {

			if (registry.sessionCount(topic) == 0) {

				mqttGatewayService.unsubscribe(topic);

			}
		}
	}

	/**
	 * Publish playback request.
	 */
	public void publish(String topic, String payload) {

		mqttGatewayService.publish(topic, payload);

	}

}