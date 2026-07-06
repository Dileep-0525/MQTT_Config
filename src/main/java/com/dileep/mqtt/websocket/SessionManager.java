package com.dileep.mqtt.websocket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SessionManager {

	/**
	 * Session -> Context
	 */
	private final Map<WebSocketSession, SessionContext> sessions = new ConcurrentHashMap<>();

	/**
	 * Register new websocket session.
	 */
	public SessionContext register(WebSocketSession session) {
		SessionContext context = new SessionContext(session);
		sessions.put(session, context);
		return context;
	}

	/**
	 * Remove websocket session.
	 */
	public void unregister(WebSocketSession session) {
		SessionContext context = sessions.remove(session);
		if (context != null) {
			context.stop();
		}
	}

	/**
	 * Queue websocket message.
	 */
	public void enqueue(WebSocketSession session, TextMessage message) {
		SessionContext context = sessions.get(session);
		if (context == null) {
			return;
		}
		context.enqueue(message);
	}

	/**
	 * Lookup context.
	 */
	public SessionContext getContext(WebSocketSession session) {
		return sessions.get(session);
	}

}