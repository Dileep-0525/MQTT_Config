package com.dileep.mqtt.websocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.Getter;

@Getter
public class SessionContext {

	/**
	 * Actual websocket session
	 */
	private final WebSocketSession session;
	/**
	 * Queue of pending websocket messages.
	 */
	private final BlockingQueue<TextMessage> queue;
	/**
	 * Indicates sender thread should continue.
	 */
	private final AtomicBoolean running;

	/**
	 * Sender thread reference.
	 */
	private Thread senderThread;

	public SessionContext(WebSocketSession session) {
		this.session = session;
		/**
		 * Capacity can be tuned later.
		 */
		this.queue = new LinkedBlockingQueue<>(100);
		this.running = new AtomicBoolean(true);
	}

	/**
	 * Add message into queue.
	 */
	public boolean enqueue(TextMessage message) {
		return queue.offer(message);
	}

	/**
	 * Stop sender.
	 */
	public void stop() {
		running.set(false);
		if (senderThread != null) {
			senderThread.interrupt();

		}
		queue.clear();

	}

	public void setSenderThread(Thread senderThread) {
		this.senderThread = senderThread;
	}

}