//package com.dileep.mqtt.websocket;
//
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.springframework.stereotype.Component;
//import org.springframework.web.socket.CloseStatus;
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
//import org.springframework.web.socket.handler.TextWebSocketHandler;
//
//import com.dileep.mqtt.dto.WebSocketRequest;
//import com.dileep.mqtt.util.JwtUtil;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class CameraWebSocketHandler extends TextWebSocketHandler {
//
//	/**
//	 * Keep the decorated session.
//	 */
//	private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
//
//	/**
//	 * Original session -> Decorated session
//	 */
//	private final Map<String, WebSocketSession> decoratedSessions = new ConcurrentHashMap<>();
//
//	/**
//	 * Authenticated sessions
//	 */
//	private final Map<String, Long> authenticatedSessions = new ConcurrentHashMap<>();
//
//	private final JwtUtil jwtUtil;
//
//	private final SubscriptionService subscriptionService;
//
//	private final ObjectMapper objectMapper;
//
//	private final WebSocketSender webSocketSender;
//
//	private final SessionManager sessionManager;
//
//	private final SessionSender sessionSender;
//
////	@Override
////	public void afterConnectionEstablished(WebSocketSession session) {
////
////		WebSocketSession decorated = new ConcurrentWebSocketSessionDecorator(session, 10000, 1024 * 1024);
////
////		sessions.add(decorated);
////
////		decoratedSessions.put(session.getId(), decorated);
////
////		log.info("""
////				===================================
////				WebSocket Connected
////				Session : {}
////				Connected Sessions : {}
////				===================================
////				""", session.getId(), sessions.size());
////	}
//
//	@Override
//	public void afterConnectionEstablished(WebSocketSession session) {
//		WebSocketSession concurrentSession = new ConcurrentWebSocketSessionDecorator(session, 10000, 1024 * 1024);
//		sessions.add(concurrentSession);
//		SessionContext context = sessionManager.register(concurrentSession);
//		sessionSender.start(context);
//		log.info("WebSocket Connected : {}", concurrentSession.getId());
//		log.info("Connected Sessions : {}", sessions.size());
//	}
//
//	@Override
//	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
//
//		WebSocketSession decorated = decoratedSessions.getOrDefault(session.getId(), session);
//
//		WebSocketRequest request = objectMapper.readValue(message.getPayload(), WebSocketRequest.class);
//
//		switch (request.action().toUpperCase()) {
//
//		case "AUTH" -> authenticate(decorated, request.token());
//
//		case "SUBSCRIBE" -> {
//
//			if (!isAuthenticated(decorated)) {
//
//				webSocketSender.send(decorated, new TextMessage("NOT_AUTHENTICATED"));
//
//				decorated.close();
//
//				return;
//			}
//
//			subscribe(decorated, request.topic());
//		}
//
//		case "PLAYBACK" -> {
//
//			if (!isAuthenticated(decorated)) {
//
////				webSocketSender.send(decorated, new TextMessage("NOT_AUTHENTICATED"));
//				sessionManager.enqueue(session, new TextMessage("NOT_AUTHENTICATED"));
//
//				decorated.close();
//
//				return;
//			}
//
//			String payload = objectMapper.writeValueAsString(request.payload());
//
//			subscriptionService.publish(request.topic(), payload);
//
//			webSocketSender.send(decorated, new TextMessage("PLAYBACK_REQUEST_SENT"));
//		}
//
//		case "UNSUBSCRIBE" -> {
//
//			subscriptionService.unsubscribe(decorated, request.topic());
//
//			webSocketSender.send(decorated, new TextMessage("UNSUBSCRIBED"));
//		}
//
//		default ->
//
//			log.warn("Unknown Action : {}", request.action());
//		}
//	}
//
//	private void authenticate(WebSocketSession session, String token) throws Exception {
//
//		if (!jwtUtil.validateToken(token)) {
//
////			webSocketSender.send(session, new TextMessage("AUTH_FAILED"));
//			sessionManager.enqueue(session, new TextMessage("AUTH_FAILED"));
//
//			session.close();
//
//			return;
//		}
//
//		authenticatedSessions.put(session.getId(), 1L);
//
//		log.info("Authenticated : {}", jwtUtil.extractUsername(token));
//
////		webSocketSender.send(session, new TextMessage("AUTH_SUCCESS"));
//		sessionManager.enqueue(session, new TextMessage("AUTH_SUCCESS"));
//	}
//
//	private boolean isAuthenticated(WebSocketSession session) {
//
//		return authenticatedSessions.containsKey(session.getId());
//	}
//
//	private void subscribe(WebSocketSession session, String topic) throws Exception {
//
//		subscriptionService.subscribe(session, topic);
//
////		webSocketSender.send(session, new TextMessage("SUBSCRIBED : " + topic));
//		sessionManager.enqueue(session, new TextMessage("SUBSCRIBED : " + topic));
//	}
//
////	@Override
////	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
////
////		WebSocketSession decorated = decoratedSessions.remove(session.getId());
////
////		if (decorated != null) {
////
////			sessions.remove(decorated);
////
////			subscriptionService.removeSession(decorated);
////		}
////
////		authenticatedSessions.remove(session.getId());
////
////		log.warn("""
////				===================================
////				WebSocket Closed
////				Session : {}
////				Code    : {}
////				Reason  : {}
////				===================================
////				""", session.getId(), status.getCode(), status.getReason());
////	}
//
//	@Override
//	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
//		log.warn("""
//				===============================
//				WebSocket CLOSED
//				Session Id : {}
//				Code       : {}
//				Reason     : {}
//				===============================
//				""", session.getId(), status.getCode(), status.getReason());
//		sessions.remove(session);
//		subscriptionService.removeSession(session);
//		authenticatedSessions.remove(session);
//		sessionManager.unregister(session);
//		log.info("Connected Sessions : {}", sessions.size());
//	}
//
//	@Override
//	public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
//
//		log.error("""
//				===================================
//				Transport Error
//				Session : {}
//				Error   : {}
//				===================================
//				""", session.getId(), exception.getMessage(), exception);
//
//		try {
//
//			session.close(CloseStatus.SERVER_ERROR);
//
//		} catch (Exception ignore) {
//		}
//		super.handleTransportError(session, exception);
//	}
//
//	public Set<WebSocketSession> getSessions() {
//		return sessions;
//	}
//}

package com.dileep.mqtt.websocket;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
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
	 * Connected websocket sessions.
	 */
	private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();

	/**
	 * Session Manager
	 */
	private final SessionManager sessionManager;

	/**
	 * Dedicated sender thread starter
	 */
	private final SessionSender sessionSender;

	/**
	 * Authenticated users
	 */
	private final Map<WebSocketSession, Long> authenticatedSessions = new ConcurrentHashMap<>();

	private final JwtUtil jwtUtil;

	private final SubscriptionService subscriptionService;

	private final ObjectMapper objectMapper;

	@Override
	public void afterConnectionEstablished(WebSocketSession session) {
		sessions.add(session);
		SessionContext context = sessionManager.register(session);
		sessionSender.start(context);
		log.info("WebSocket Connected : {}", session.getId());
		log.info("Connected Sessions : {}", sessions.size());
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
		log.warn("""
				===============================
				WebSocket CLOSED
				Session Id : {}
				Code       : {}
				Reason     : {}
				===============================
				""", session.getId(), status.getCode(), status.getReason());
		sessions.remove(session);
		subscriptionService.removeSession(session);
		authenticatedSessions.remove(session);
		sessionManager.unregister(session);
		log.info("Connected Sessions : {}", sessions.size());
		log.info("WebSocket Disconnected : {}", session.getId());
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		WebSocketRequest request = objectMapper.readValue(message.getPayload(), WebSocketRequest.class);
		if ("AUTH".equalsIgnoreCase(request.action())) {
			authenticate(session, request.token());
			return;
		}
		if (!isAuthenticated(session)) {

		    synchronized (session) {
		        session.sendMessage(
		                new TextMessage("NOT_AUTHENTICATED"));
		    }

		    session.close();

		    return;
		}
		if ("SUBSCRIBE".equalsIgnoreCase(request.action())) {
			subscribe(session, request.topic());
			return;
		}

		if ("PLAYBACK".equalsIgnoreCase(request.action())) {
			String payload = objectMapper.writeValueAsString(request.payload());
			subscriptionService.publish(request.topic(), payload);
			return;
		}
		log.info("Authenticated Message Received : {}", message.getPayload());
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
		log.error("""
				===============================
				WebSocket TRANSPORT ERROR
				Session Id : {}
				Error      : {}
				===============================
				""", session.getId(), exception.getMessage(), exception);
		super.handleTransportError(session, exception);
	}

	public void authenticateSession(WebSocketSession session, Long userId) {
		authenticatedSessions.put(session, userId);
	}

	public boolean isAuthenticated(WebSocketSession session) {
		return authenticatedSessions.containsKey(session);
	}

	public Long getUserId(WebSocketSession session) {
		return authenticatedSessions.get(session);
	}

//	private void subscribe(WebSocketSession session, String topic) throws Exception {
//		subscriptionService.subscribe(session, topic);
//		sessionManager.enqueue(session, new TextMessage("SUBSCRIBED : " + topic));
//		synchronized (session) {
//		    session.sendMessage(new TextMessage("AUTH_SUCCESS"));
//		}
//	}
	
	private void subscribe(
	        WebSocketSession session,
	        String topic) throws Exception {

	    subscriptionService.subscribe(session, topic);

	    synchronized (session) {
	        session.sendMessage(
	                new TextMessage("SUBSCRIBED : " + topic));
	    }
	}

//	private void authenticate(WebSocketSession session, String token) throws Exception {
//		if (!jwtUtil.validateToken(token)) {
//			sessionManager.enqueue(session, new TextMessage("AUTH_FAILED"));
//			synchronized (session) {
//			    session.sendMessage(new TextMessage("AUTH_SUCCESS"));
//			}
//			session.close();
//			return;
//		}
//		String username = jwtUtil.extractUsername(token);
//		authenticateSession(session, 1L);
//		log.info("Authenticated User : {}", username);
//		sessionManager.enqueue(session, new TextMessage("AUTH_SUCCESS"));
//		synchronized (session) {
//		    session.sendMessage(new TextMessage("AUTH_SUCCESS"));
//		}
//	}
	private void authenticate(WebSocketSession session, String token) throws Exception {

	    if (!jwtUtil.validateToken(token)) {

	        synchronized (session) {
	            session.sendMessage(new TextMessage("AUTH_FAILED"));
	        }

	        session.close();

	        return;
	    }

	    String username = jwtUtil.extractUsername(token);

	    authenticateSession(session, 1L);

	    log.info("Authenticated User : {}", username);

	    synchronized (session) {
	        session.sendMessage(new TextMessage("AUTH_SUCCESS"));
	    }
	}

	public Set<WebSocketSession> getSessions() {
		return sessions;
	}

}
