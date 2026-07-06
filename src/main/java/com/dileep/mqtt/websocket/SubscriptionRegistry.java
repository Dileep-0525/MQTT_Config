//package com.dileep.mqtt.websocket;
//
//import java.util.Collections;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.springframework.stereotype.Component;
//import org.springframework.web.socket.WebSocketSession;
//
//@Component
//public class SubscriptionRegistry {
//
//	/**
//	 * topic -> sessions
//	 */
//	private final Map<String, Set<WebSocketSession>> topicSessions = new ConcurrentHashMap<>();
//
//	/**
//	 * session -> topics
//	 */
//	private final Map<WebSocketSession, Set<String>> sessionTopics = new ConcurrentHashMap<>();
//
//	/**
//	 * Subscribe session to topic.
//	 *
//	 * @return true if first subscriber
//	 */
//	public boolean subscribe(WebSocketSession session, String topic) {
//
//		topicSessions.computeIfAbsent(topic, t -> ConcurrentHashMap.newKeySet()).add(session);
//
//		sessionTopics.computeIfAbsent(session, s -> ConcurrentHashMap.newKeySet()).add(topic);
//
//		return topicSessions.get(topic).size() == 1;
//	}
//
//	/**
//	 * Returns COPY of sessions.
//	 */
//	public Set<WebSocketSession> getSessions(String topic) {
//
//		Set<WebSocketSession> sessions = topicSessions.get(topic);
//
//		if (sessions == null) {
//			return Collections.emptySet();
//		}
//
//		return Set.copyOf(sessions);
//	}
//
//	/**
//	 * Unsubscribe session from topic.
//	 *
//	 * @return true if last subscriber removed
//	 */
//	public boolean unsubscribe(WebSocketSession session, String topic) {
//
//		Set<WebSocketSession> sessions = topicSessions.get(topic);
//
//		if (sessions != null) {
//
//			sessions.remove(session);
//
//			if (sessions.isEmpty()) {
//
//				topicSessions.remove(topic);
//			}
//		}
//
//		Set<String> topics = sessionTopics.get(session);
//
//		if (topics != null) {
//
//			topics.remove(topic);
//
//			if (topics.isEmpty()) {
//
//				sessionTopics.remove(session);
//			}
//		}
//
//		return sessions == null || sessions.isEmpty();
//	}
//
//	/**
//	 * Remove session from ALL topics.
//	 */
//	public Set<String> removeSession(WebSocketSession session) {
//
//		Set<String> topics = sessionTopics.remove(session);
//
//		if (topics == null) {
//			return Collections.emptySet();
//		}
//
//		for (String topic : topics) {
//
//			Set<WebSocketSession> sessions = topicSessions.get(topic);
//
//			if (sessions != null) {
//
//				sessions.remove(session);
//
//				if (sessions.isEmpty()) {
//
//					topicSessions.remove(topic);
//				}
//			}
//		}
//
//		return Set.copyOf(topics);
//	}
//
//	/**
//	 * Returns all topics subscribed by session.
//	 */
//	public Set<String> getTopics(WebSocketSession session) {
//
//		return Set.copyOf(sessionTopics.getOrDefault(session, Collections.emptySet()));
//	}
//
//	/**
//	 * Registry size.
//	 */
//	public int topicCount() {
//
//		return topicSessions.size();
//	}
//
//	/**
//	 * Session count.
//	 */
//	public int sessionCount() {
//
//		return sessionTopics.size();
//	}
//
//	/**
//	 * For debugging.
//	 */
//	public void clear() {
//
//		topicSessions.clear();
//		sessionTopics.clear();
//	}
//}

package com.dileep.mqtt.websocket;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

@Component
public class SubscriptionRegistry {

	/**
	 * Topic -> Sessions
	 */
	private final Map<String, Set<WebSocketSession>> topicSessions = new ConcurrentHashMap<>();

	/**
	 * Session -> Topics
	 */
	private final Map<WebSocketSession, Set<String>> sessionTopics = new ConcurrentHashMap<>();

	/**
	 * Register subscription.
	 *
	 * @return true if this is the first subscriber for the topic.
	 */
	public boolean subscribe(WebSocketSession session, String topic) {
		Set<WebSocketSession> sessions = topicSessions.computeIfAbsent(topic, key -> ConcurrentHashMap.newKeySet());
		sessions.add(session);
		sessionTopics.computeIfAbsent(session, key -> ConcurrentHashMap.newKeySet()).add(topic);
		return sessions.size() == 1;
	}

	/**
	 * Get subscribers for topic.
	 */
	public Set<WebSocketSession> getSessions(String topic) {
		return topicSessions.getOrDefault(topic, Collections.emptySet());
	}

	/**
	 * Remove one subscription.
	 *
	 * @return true if topic has no subscribers.
	 */
	public boolean unsubscribe(WebSocketSession session, String topic) {
		Set<WebSocketSession> sessions = topicSessions.get(topic);
		if (sessions != null) {
			sessions.remove(session);
			if (sessions.isEmpty()) {
				topicSessions.remove(topic);
			}
		}
		Set<String> topics = sessionTopics.get(session);
		if (topics != null) {
			topics.remove(topic);
			if (topics.isEmpty()) {
				sessionTopics.remove(session);
			}
		}
		return sessions == null || sessions.isEmpty();
	}

	/**
	 * Remove complete websocket session.
	 */
	public Set<String> removeSession(WebSocketSession session) {
		Set<String> topics = sessionTopics.remove(session);
		if (topics == null) {
			return Collections.emptySet();
		}
		for (String topic : topics) {
			Set<WebSocketSession> sessions = topicSessions.get(topic);
			if (sessions != null) {
				sessions.remove(session);
				if (sessions.isEmpty()) {
					topicSessions.remove(topic);
				}
			}
		}
		return topics;
	}

	/**
	 * Number of subscribers.
	 */
	public int sessionCount(String topic) {
		Set<WebSocketSession> sessions = topicSessions.get(topic);
		return sessions == null ? 0 : sessions.size();
	}

	/**
	 * Debug only.
	 */
	public int totalSessions() {
		return sessionTopics.size();
	}

	/**
	 * Debug only.
	 */
	public int totalTopics() {
		return topicSessions.size();
	}

}
