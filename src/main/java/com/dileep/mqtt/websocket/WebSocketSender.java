//package com.dileep.mqtt.websocket;
//
//import org.springframework.stereotype.Component;
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//@Component
//public class WebSocketSender {
//
//	public void send(WebSocketSession session, TextMessage message) {
//		if (session == null) {
//			return;
//		}
//		if (!session.isOpen()) {
//			return;
//		}
//		synchronized (session) {
//			if (!session.isOpen()) {
//				return;
//			}
//			try {
//				session.sendMessage(message);
//			} catch (Exception ex) {
//				log.warn("Failed sending message to session {}", session.getId(), ex);
//			}
//		}
//	}
//}