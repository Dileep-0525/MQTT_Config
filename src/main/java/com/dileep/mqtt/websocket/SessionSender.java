package com.dileep.mqtt.websocket;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SessionSender {

	public void start(SessionContext context) {
		Thread sender = Thread.ofVirtual().start(() -> {
			WebSocketSession session = context.getSession();
			try {
				while (context.getRunning().get()) {
					/*
					 * Sleep until some message arrives.
					 */
					context.getSignal().acquire();
					if (!session.isOpen()) {
						break;
					}
					/*
					 * Priority 1 : Live Stream
					 */
					TextMessage message = context.pollLive();
					/*
					 * Priority 2 : Playback
					 */
					if (message == null) {
						message = context.getPlaybackQueue().poll();
					}
					/*
					 * Priority 3 : Events
					 */
					if (message == null) {
						message = context.getControlQueue().poll();
					}

					if (message == null) {
						continue;
					}
					try {
						session.sendMessage(message);
						/*
						 * Successful send.
						 */
						context.getWriteTimeouts().set(0);
					} catch (IllegalStateException ex) {

						log.debug("Session already closed : {}", session.getId());
						break;
					} catch (IOException ex) {
						/*
						 * Browser is slow.
						 */
						if (isWriteTimeout(ex)) {
							int failures = context.getWriteTimeouts().incrementAndGet();
							log.warn("Write timeout {} for session {}", failures, session.getId());
							/*
							 * Allow a few consecutive timeouts.
							 */
							if (failures < 5) {
								continue;
							}
							log.warn("Too many write timeouts. Closing session : {}", session.getId());
							break;
						}

						/*
						 * Browser disconnected.
						 */
						if (isDisconnected(ex)) {
							log.info("Client disconnected : {}", session.getId());
							break;
						}
						log.warn("Unexpected websocket error : {}", session.getId(), ex);
						break;
					}
				}
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			} finally {
				context.stop();
				log.info("Sender stopped : {}", session.getId());
			}

		});

		context.setSenderThread(sender);
	}

	private boolean isWriteTimeout(Throwable ex) {
		while (ex != null) {
			if (ex instanceof SocketTimeoutException) {
				return true;
			}
			ex = ex.getCause();
		}
		return false;
	}

	private boolean isDisconnected(Throwable ex) {
		while (ex != null) {
			String msg = ex.getMessage();
			if (msg != null) {
				msg = msg.toLowerCase();
				if (msg.contains("connection reset") || msg.contains("broken pipe")
						|| msg.contains("transformer has been closed") || msg.contains("deflater has been closed")
						|| msg.contains("closed")) {

					return true;
				}
			}
			ex = ex.getCause();
		}

		return false;
	}
}
