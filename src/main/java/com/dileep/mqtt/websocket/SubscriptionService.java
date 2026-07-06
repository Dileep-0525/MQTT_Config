//package com.dileep.mqtt.websocket;
//
//import java.util.Set;
//
//import org.springframework.stereotype.Service;
//import org.springframework.web.socket.WebSocketSession;
//
//import com.dileep.mqtt.service.MqttGatewayService;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//@Service
//@RequiredArgsConstructor
//public class SubscriptionService {
//
//	private final SubscriptionRegistry registry;
//
//	private final MqttGatewayService mqttGatewayService;
//
//	/**
//	 * Subscribe websocket session to topic.
//	 */
//	public void subscribe(WebSocketSession session, String topic) {
//
//		boolean firstSubscriber = registry.subscribe(session, topic);
//
//		log.debug("Session {} subscribed to {}", session.getId(), topic);
//
//		if (firstSubscriber) {
//
//			try {
//
//				mqttGatewayService.subscribe(topic);
//
//				log.info("MQTT Subscribe : {}", topic);
//
//			} catch (Exception ex) {
//
//				log.error("MQTT subscribe failed : {}", topic, ex);
//
//				registry.unsubscribe(session, topic);
//			}
//		}
//	}
//
//	/**
//	 * Unsubscribe websocket session from topic.
//	 */
//	public void unsubscribe(WebSocketSession session, String topic) {
//
//		boolean lastSubscriber = registry.unsubscribe(session, topic);
//
//		log.debug("Session {} unsubscribed from {}", session.getId(), topic);
//
//		if (lastSubscriber) {
//
//			try {
//
//				mqttGatewayService.unsubscribe(topic);
//
//				log.info("MQTT Unsubscribe : {}", topic);
//
//			} catch (Exception ex) {
//
//				log.error("MQTT unsubscribe failed : {}", topic, ex);
//			}
//		}
//	}
//
//	/**
//	 * Publish MQTT message.
//	 */
//	public void publish(String topic, String payload) {
//
//		try {
//
//			mqttGatewayService.publish(topic, payload);
//
//		} catch (Exception ex) {
//
//			log.error("MQTT Publish failed : {}", topic, ex);
//		}
//	}
//
//	/**
//	 * Remove websocket session from all topics.
//	 */
//	public void removeSession(WebSocketSession session) {
//
//		Set<String> topics = registry.removeSession(session);
//
//		if (topics.isEmpty()) {
//			return;
//		}
//
//		log.info("Removing session {} from {} topics", session.getId(), topics.size());
//
//		for (String topic : topics) {
//
//			if (registry.getSessions(topic).isEmpty()) {
//
//				try {
//
//					mqttGatewayService.unsubscribe(topic);
//
//					log.info("MQTT Unsubscribe : {}", topic);
//
//				} catch (Exception ex) {
//
//					log.error("MQTT unsubscribe failed : {}", topic, ex);
//				}
//			}
//		}
//	}
//}

package com.dileep.mqtt.websocket;

import java.util.Set;

import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import com.dileep.mqtt.service.MqttGatewayService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
			log.info("MQTT Subscribe : {}", topic);
			mqttGatewayService.subscribe(topic);
		}
	}

	/**
	 * Unsubscribe websocket session.
	 */
	public void unsubscribe(WebSocketSession session, String topic) {
		boolean lastSubscriber = registry.unsubscribe(session, topic);
		if (lastSubscriber) {
			log.info("MQTT Unsubscribe : {}", topic);
			mqttGatewayService.unsubscribe(topic);
		}

	}

	/**
	 * Publish MQTT message.
	 */
	public void publish(String topic, String payload) {
		mqttGatewayService.publish(topic, payload);
	}

	/**
	 * Remove websocket session completely.
	 */
	public void removeSession(WebSocketSession session) {
		Set<String> topics = registry.removeSession(session);
		for (String topic : topics) {
			if (registry.sessionCount(topic) == 0) {
				log.info("MQTT Unsubscribe : {}", topic);
				mqttGatewayService.unsubscribe(topic);
			}
		}
	}

}
