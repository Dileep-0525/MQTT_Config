package com.dileep.mqtt.websocket;

import java.io.IOException;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SessionSender {

	/**
	 * Starts one dedicated sender thread per WebSocket session.
	 */
	public void start(SessionContext context) {
		Thread sender = Thread.ofVirtual().start(() -> {
			WebSocketSession session = context.getSession();
			try {
				while (context.getRunning().get()) {
					/*
					 * Wait until a message is available.
					 */
					TextMessage message = context.getQueue().take();
					/*
					 * Session already closed.
					 */
					if (!session.isOpen()) {
						break;
					}
					try {
						synchronized (session) {
							session.sendMessage(message);
						}
					} catch (IllegalStateException ex) {
						/*
						 * Client disconnected while sending.
						 */
						log.debug("Session already closed : {}", session.getId());
						break;
					} catch(IOException ex){

					    log.warn("Write failed {}", session.getId());

					    if(!session.isOpen()){
					        break;
					    }

					}
				}
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			} finally {

			    context.stop();

			    context.getQueue().clear();

			    log.info("Sender stopped {}", session.getId());

			}
		});
		context.setSenderThread(sender);
	}

}