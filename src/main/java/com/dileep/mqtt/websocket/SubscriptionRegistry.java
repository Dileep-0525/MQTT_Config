package com.dileep.mqtt.websocket;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
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

	public boolean unsubscribe(WebSocketSession session, String topic) {
	    Set<WebSocketSession> sessions = topicSessions.get(topic);
	    if (sessions != null) {
	        sessions.remove(session);
	        log.info("Session [{}] unsubscribed from topic [{}]", session.getId(), topic);
	        log.info("Remaining subscribers for [{}] : {}", topic, sessions.size());
	        if (sessions.isEmpty()) {
	            topicSessions.remove(topic);
	            log.info("Last subscriber removed for topic [{}]", topic);
	        }
	    }

	    Set<String> topics = sessionTopics.get(session);
	    if (topics != null) {
	        topics.remove(topic);
	        if (topics.isEmpty()) {
	            sessionTopics.remove(session);
	            log.info("Session [{}] removed from session registry", session.getId());
	        }
	    }

	    return sessions == null || sessions.isEmpty();
	}
	
	
	public Set<String> removeSession(WebSocketSession session) {
	    log.info("Removing complete session [{}]", session.getId());
	    Set<String> topics = sessionTopics.remove(session);
	    if (topics == null) {
	        log.info("No topics found for session [{}]", session.getId());
	        return Collections.emptySet();
	    }
	    for (String topic : topics) {
	        Set<WebSocketSession> sessions = topicSessions.get(topic);
	        if (sessions != null) {
	            sessions.remove(session);
	            log.info("Removed session [{}] from topic [{}]", session.getId(), topic);
	            log.info("Remaining subscribers for [{}] : {}", topic, sessions.size());
	            if (sessions.isEmpty()) {
	                topicSessions.remove(topic);
	                log.info("Topic [{}] has no more subscribers", topic);
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
