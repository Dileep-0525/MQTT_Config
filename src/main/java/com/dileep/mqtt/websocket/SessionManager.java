//package com.dileep.mqtt.websocket;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.springframework.stereotype.Component;
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//
//@Component
//public class SessionManager {
//
//	/**
//	 * Session -> Context
//	 */
//	private final Map<WebSocketSession, SessionContext> sessions = new ConcurrentHashMap<>();
//
//	/**
//	 * Register new websocket session.
//	 */
//	public SessionContext register(WebSocketSession session) {
//		SessionContext context = new SessionContext(session);
//		sessions.put(session, context);
//		return context;
//	}
//
//	/**
//	 * Remove websocket session.
//	 */
//	public void unregister(WebSocketSession session) {
//		SessionContext context = sessions.remove(session);
//		if (context != null) {
//			context.stop();
//		}
//	}
//
//	/**
//	 * Enqueue Live Stream message.
//	 */
//	public void enqueueLive(WebSocketSession session, TextMessage message) {
//		SessionContext context = sessions.get(session);
//		if (context == null) {
//			return;
//		}
//		context.enqueueLive(message);
//	}
//
//	/**
//	 * Enqueue Playback message.
//	 */
//	public void enqueuePlayback(WebSocketSession session, TextMessage message) {
//		SessionContext context = sessions.get(session);
//		if (context == null) {
//			return;
//		}
//		context.enqueuePlayback(message);
//	}
//
//	/**
//	 * Enqueue Event message.
//	 */
//	public void enqueueEvent(WebSocketSession session, TextMessage message) {
//		SessionContext context = sessions.get(session);
//		if (context == null) {
//			return;
//		}
//		context.enqueueEvent(message);
//	}
//
//	/**
//	 * Lookup context.
//	 */
//	public SessionContext getContext(WebSocketSession session) {
//		return sessions.get(session);
//	}
//
//}
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
    private final Map<WebSocketSession, SessionContext> sessions =
            new ConcurrentHashMap<>();

    /**
     * One sender thread per session.
     */
    private final SessionSender sessionSender;

    /**
     * Register websocket session.
     */
    public SessionContext register(WebSocketSession session) {

        SessionContext context = new SessionContext(session);

        sessions.put(session, context);

        sessionSender.start(context);

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
     * Live video.
     */
    public void enqueueLive(
            WebSocketSession session,
            TextMessage message) {

        SessionContext context = sessions.get(session);

        if (context != null) {
            context.enqueueLive(message);
        }
    }
    
//    public void enqueue(
//            WebSocketSession session,
//            TextMessage message) {
//
//        SessionContext context = sessions.get(session);
//
//        if (context != null) {
//            context.enqueueLive(message);
//        }
//    }

    /**
     * Playback frames.
     */
    public void enqueuePlayback(
            WebSocketSession session,
            TextMessage message) {

        SessionContext context = sessions.get(session);

        if (context != null) {
            context.enqueuePlayback(message);
        }
    }

    /**
     * Control messages.
     *
     * AUTH_SUCCESS
     * AUTH_FAILED
     * SUBSCRIBED
     * PLAYBACK_REQUEST_SENT
     * MQTT Events
     */
    public void enqueueControl(
            WebSocketSession session,
            TextMessage message) {

        SessionContext context = sessions.get(session);

        if (context != null) {
            context.enqueueControl(message);
        }
    }

    /**
     * Lookup context.
     */
    public SessionContext getContext(WebSocketSession session) {

        return sessions.get(session);
    }
    
    
    /**
     * All active session contexts.
     */
    public Iterable<SessionContext> getAllContexts() {
        return sessions.values();
    }

    /**
     * Number of connected websocket sessions.
     */
    public int size() {
        return sessions.size();
    }
    

}