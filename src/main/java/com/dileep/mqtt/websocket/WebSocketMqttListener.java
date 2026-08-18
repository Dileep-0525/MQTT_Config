package com.dileep.mqtt.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.dileep.mqtt.service.MqttMessageListener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketMqttListener implements MqttMessageListener {

	private final SubscriptionRegistry subscriptionRegistry;

	private final SessionManager sessionManager;

//	private final ObjectMapper objectMapper;

	@Override
	public void onMessage(String topic, String payload) {
		try {
//			/*
//			 * Create response only once.
//			 */
//			WebSocketResponse response = new WebSocketResponse("MQTT_MESSAGE", topic, objectMapper.readTree(payload));
//			/*
//			 * Serialize only once.
//			 */
//			TextMessage message = new TextMessage(objectMapper.writeValueAsString(response));

			StringBuilder json = new StringBuilder(payload.length() + topic.length() + 64);

			json.append("{\"type\":\"MQTT_MESSAGE\",\"topic\":\"").append(topic).append("\",\"payload\":")
					.append(payload).append('}');

			TextMessage message = new TextMessage(json.toString());

			/*
			 * Fan-out to all subscribed sessions.
			 */
			for (WebSocketSession session : subscriptionRegistry.getSessions(topic)) {
				if (!session.isOpen()) {
					subscriptionRegistry.removeSession(session);
					sessionManager.unregister(session);
					continue;
				}
				if (isLiveTopic(topic)) {
					sessionManager.enqueueLive(session, message);
				} else if (isPlaybackTopic(topic)) {
					sessionManager.enqueuePlayback(session, message);
				} else if (isEventTopic(topic)) {
					sessionManager.enqueueControl(session, message);
				} else {
					log.warn("Unknown MQTT topic : {}", topic);
				}
//				sessionManager.enqueue(session, message);
			}
		} catch (Exception ex) {
			log.error("Failed processing MQTT message for topic {}", topic, ex);

		}
	}

	/**
	 * Live inference topics.
	 */
	private boolean isLiveTopic(String topic) {
		return topic.startsWith("live/") && topic.endsWith("/vision-inference");
	}

	/**
	 * Playback topics.
	 */
	private boolean isPlaybackTopic(String topic) {
		return topic.startsWith("hist/");
	}

	/**
	 * Event topics.
	 */
	private boolean isEventTopic(String topic) {
		return topic.startsWith("live/") && topic.endsWith("/events");
	}

//	public void onMessage(String topic,String message) {
//		try {
//
//			/*
//			 * Fan-out to all subscribed sessions.
//			 */
//			for (WebSocketSession session : subscriptionRegistry.getSessions(topic)) {
//				if (!session.isOpen()) {
//					subscriptionRegistry.removeSession(session);
//					sessionManager.unregister(session);
//					continue;
//				}
//				sessionManager.enqueue(session, message);
//			}
//		} catch (Exception ex) {
//			log.error("Failed processing MQTT message for topic {}",  ex);
//
//		}	// TODO Auto-generated method stub
//		
//	}

}
