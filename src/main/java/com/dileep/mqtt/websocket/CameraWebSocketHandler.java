package com.dileep.mqtt.websocket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.dileep.mqtt.dto.WebSocketRequest;
import com.dileep.mqtt.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class CameraWebSocketHandler extends TextWebSocketHandler {

	/**
	 * SessionId -> Concurrent Session
	 */
	private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

	private volatile long lastPongTime = System.currentTimeMillis();
	
	/**
	 * Session -> UserId
	 */
	private final Map<WebSocketSession, Long> authenticatedSessions = new ConcurrentHashMap<>();

	private final JwtUtil jwtUtil;

	private final SubscriptionService subscriptionService;

	private final ObjectMapper objectMapper;

	private final SessionManager sessionManager;

	@Override
	public void afterConnectionEstablished(WebSocketSession session) {

		ConcurrentWebSocketSessionDecorator concurrentSession = new ConcurrentWebSocketSessionDecorator(session, 60000,
				10 * 1024 * 1024);

		sessions.put(concurrentSession.getId(), concurrentSession);

		sessionManager.register(concurrentSession);

		SessionContext context = sessionManager.getContext(concurrentSession);

		context.setLastPongTime(System.currentTimeMillis());

		log.info("WebSocket Connected : {}", concurrentSession.getId());
		log.info("Connected Sessions : {}", sessions.size());
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {

		WebSocketSession actual = sessions.remove(session.getId());

		if (actual != null) {

			authenticatedSessions.remove(actual);

			subscriptionService.removeSession(actual);

			sessionManager.unregister(actual);
		}

		log.info("WebSocket Disconnected : {}", session.getId());
		log.info("Connected Sessions : {}", sessions.size());
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {

		WebSocketSession actual = getActualSession(session);

		WebSocketRequest request = objectMapper.readValue(message.getPayload(), WebSocketRequest.class);

		switch (request.action().toUpperCase()) {

		case "AUTH":

			authenticate(actual, request.token());

			break;

		case "SUBSCRIBE":

			if (!isAuthenticated(actual)) {

				synchronized (actual) {
					actual.sendMessage(new TextMessage("NOT_AUTHENTICATED"));
				}

				actual.close();

				return;
			}

			subscribe(actual, request.topic());

			break;

		case "PLAYBACK":

			if (!isAuthenticated(actual)) {

				synchronized (actual) {
					actual.sendMessage(new TextMessage("NOT_AUTHENTICATED"));
				}

				return;
			}

			playback(request);

			synchronized (actual) {
				actual.sendMessage(new TextMessage("PLAYBACK_REQUEST_SENT"));
			}

			break;

		default:

			log.warn("Unknown action : {}", request.action());
		}
	}

	private void authenticate(WebSocketSession session, String token) throws Exception {
		WebSocketSession actual = getActualSession(session);
		if (!jwtUtil.validateToken(token)) {
			synchronized (actual) {
				actual.sendMessage(new TextMessage("AUTH_FAILED"));
			}
			actual.close();
			return;
		}
		authenticateSession(actual, 1L);
		log.info("Authenticated User : {}", jwtUtil.extractUsername(token));
		synchronized (actual) {
			actual.sendMessage(new TextMessage("AUTH_SUCCESS"));
		}
	}

//	private void subscribe(WebSocketSession session, String topic) {
//
//		subscriptionService.subscribe(session, topic);
//
//		enqueue(session, "SUBSCRIBED : " + topic);
//	}

	private void subscribe(WebSocketSession session, String topic) throws Exception {

		WebSocketSession actual = getActualSession(session);

		subscriptionService.subscribe(actual, topic);

		synchronized (actual) {
			actual.sendMessage(new TextMessage("SUBSCRIBED : " + topic));
		}
	}

	private void playback(WebSocketRequest request) throws Exception {

		String payload = objectMapper.writeValueAsString(request.payload());

		subscriptionService.publish(request.topic(), payload);
	}

	/**
	 * Enqueue control message.
	 */
//	private void enqueue(WebSocketSession session, String message) {
//
//		sessionManager.enqueueControl(session, new TextMessage(message));
//	}

	public void authenticateSession(WebSocketSession session, Long userId) {

		authenticatedSessions.put(session, userId);
	}

	public boolean isAuthenticated(WebSocketSession session) {

		return authenticatedSessions.containsKey(session);
	}

	public Long getUserId(WebSocketSession session) {

		return authenticatedSessions.get(session);
	}

	private WebSocketSession getActualSession(WebSocketSession session) {

		return sessions.getOrDefault(session.getId(), session);
	}

	public WebSocketSession getSession(String sessionId) {

		return sessions.get(sessionId);
	}

	@Override
	protected void handlePongMessage(WebSocketSession session, PongMessage message) throws Exception {

	    SessionContext context = sessionManager.getContext(session);

	    if (context != null) {
	        context.setLastPongTime(System.currentTimeMillis());

	        log.debug("PONG received from {}", session.getId());
	    }
	}

}