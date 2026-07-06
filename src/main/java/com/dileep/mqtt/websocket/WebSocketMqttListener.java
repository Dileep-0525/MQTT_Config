//package com.dileep.mqtt.websocket;
//
//import java.util.Set;
//
//import org.springframework.stereotype.Component;
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//
//import com.dileep.mqtt.dto.WebSocketResponse;
//import com.dileep.mqtt.service.MqttMessageListener;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class WebSocketMqttListener implements MqttMessageListener {
//
//    private final SubscriptionRegistry subscriptionRegistry;
//
//    private final WebSocketSender webSocketSender;
//
//    private final ObjectMapper objectMapper;
//
//    @Override
//    public void onMessage(
//            String topic,
//            String payload) {
//
//        Set<WebSocketSession> sessions =
//                subscriptionRegistry.getSessions(topic);
//
//        if (sessions.isEmpty()) {
//            return;
//        }
//
//        try {
//
//            WebSocketResponse response =
//                    new WebSocketResponse(
//                            "MQTT_MESSAGE",
//                            topic,
//                            objectMapper.readTree(payload));
//
//            TextMessage message =
//                    new TextMessage(
//                            objectMapper.writeValueAsString(response));
//
//            for (WebSocketSession session : sessions) {
//
//                if (!session.isOpen()) {
//
//                    subscriptionRegistry.removeSession(session);
//
//                    continue;
//                }
//
//                try {
//
//                    webSocketSender.send(session, message);
//
//                } catch (Exception ex) {
//
//                    log.warn(
//                            "Removing closed session {}",
//                            session.getId());
//
//                    subscriptionRegistry.removeSession(session);
//
//                    try {
//
//                        session.close();
//
//                    } catch (Exception ignore) {
//                    }
//                }
//            }
//
//        } catch (Exception ex) {
//
//            log.error(
//                    "Failed processing MQTT message",
//                    ex);
//        }
//    }
//}

package com.dileep.mqtt.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.dileep.mqtt.dto.WebSocketResponse;
import com.dileep.mqtt.service.MqttMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketMqttListener implements MqttMessageListener {

	private final SubscriptionRegistry subscriptionRegistry;

	private final SessionManager sessionManager;

	private final ObjectMapper objectMapper;

	@Override
	public void onMessage(String topic, String payload) {
		WebSocketResponse response;
		try {
			response = new WebSocketResponse("MQTT_MESSAGE", topic, objectMapper.readTree(payload));
		} catch (Exception ex) {
			log.error("Unable to parse mqtt payload", ex);
			return;
		}
		TextMessage message;
		try {
			message = new TextMessage(objectMapper.writeValueAsString(response));
		} catch (Exception ex) {
			log.error("Unable to serialize websocket response", ex);
			return;
		}

		for (WebSocketSession session : subscriptionRegistry.getSessions(topic)) {
			if (!session.isOpen()) {
				subscriptionRegistry.removeSession(session);
				sessionManager.unregister(session);
				continue;
			}
			sessionManager.enqueue(session, message);
		}
	}

}
