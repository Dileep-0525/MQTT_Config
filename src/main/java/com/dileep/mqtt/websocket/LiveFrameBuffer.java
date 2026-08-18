//package com.dileep.mqtt.websocket;
//
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.springframework.web.socket.TextMessage;
//
//public class LiveFrameBuffer {
//
//	/**
//	 * Frames older than this will be discarded. Adjust according to your latency
//	 * requirement.
//	 */
//	private static final long MAX_FRAME_AGE_MS = 200;
//
//	/**
//	 * Prevent unlimited memory growth.
//	 */
//	private static final int MAX_QUEUE_SIZE = 100;
//
//	private final ConcurrentLinkedQueue<Frame> queue = new ConcurrentLinkedQueue<>();
//
//	private final AtomicLong droppedFrames = new AtomicLong();
//
//	/**
//	 * Add a new live frame.
//	 */
//	public void offer(TextMessage message) {
//		if (queue.size() >= MAX_QUEUE_SIZE) {
//			queue.poll();
//			droppedFrames.incrementAndGet();
//		}
//		queue.offer(new Frame(message, System.currentTimeMillis()));
//	}
//
//	/**
//	 * Returns the newest valid frame. Old frames are discarded automatically.
//	 */
//	public TextMessage poll() {
//		long now = System.currentTimeMillis();
//		while (true) {
//			Frame frame = queue.peek();
//			if (frame == null) {
//				return null;
//			}
//			/*
//			 * Frame too old.
//			 */
//			if (now - frame.timestamp > MAX_FRAME_AGE_MS) {
//				queue.poll();
//				droppedFrames.incrementAndGet();
//				continue;
//			}
//
//			/*
//			 * Keep only the newest frame.
//			 */
//			while (queue.size() > 1) {
//				queue.poll();
//				droppedFrames.incrementAndGet();
//			}
//			Frame latest = queue.poll();
//			return latest == null ? null : latest.message;
//		}
//	}
//
//	public void clear() {
//		queue.clear();
//	}
//
//	public long getDroppedFrames() {
//		return droppedFrames.get();
//	}
//
//	public int size() {
//		return queue.size();
//	}
//
//	/**
//	 * Internal frame wrapper.
//	 */
//	private record Frame(TextMessage message, long timestamp) {
//	}
//
//}

package com.dileep.mqtt.websocket;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.socket.TextMessage;

public class LiveFrameBuffer {

	/**
	 * Maximum acceptable live-stream latency.
	 *
	 * Frames older than this are no longer useful and are discarded.
	 *
	 * Start with 500 ms. Reduce later if lower latency is required.
	 */
	private static final long MAX_FRAME_AGE_MS = 700;

	/**
	 * Maximum number of live frames retained per WebSocket session.
	 *
	 * This prevents unlimited heap growth when a browser is slow.
	 *
	 * For a 5 FPS stream: 10 frames represents approximately 2 seconds.
	 */
	private static final int MAX_QUEUE_SIZE = 10;

	/**
	 * Live frames in MQTT arrival order.
	 */
	private final ConcurrentLinkedQueue<Frame> queue = new ConcurrentLinkedQueue<>();

	/**
	 * Queue count without repeatedly calling ConcurrentLinkedQueue.size().
	 */
	private final AtomicInteger queueSize = new AtomicInteger();

	/**
	 * Number of frames discarded.
	 */
	private final AtomicLong droppedFrames = new AtomicLong();

	/**
	 * Add a new live frame.
	 */
	public void offer(TextMessage message) {
		if (message == null) {
			return;
		}
		long now = System.currentTimeMillis();

		/*
		 * Remove expired frames before adding a new frame.
		 */
		removeExpiredFrames(now);

		/*
		 * If the queue reached its maximum capacity, remove the oldest frame.
		 *
		 * Never remove the newest frame.
		 */
		while (queueSize.get() >= MAX_QUEUE_SIZE) {
			Frame removed = queue.poll();
			if (removed == null) {
				queueSize.set(0);
				break;
			}
			queueSize.decrementAndGet();
			droppedFrames.incrementAndGet();
		}
		/*
		 * Store the new frame with its arrival timestamp.
		 */
		queue.offer(new Frame(message, now));
		queueSize.incrementAndGet();
	}

	/**
	 * Return the oldest valid frame.
	 *
	 * Frames are sent in MQTT arrival order. Only expired frames are discarded.
	 */
	public TextMessage poll() {
		long now = System.currentTimeMillis();
		while (true) {
			Frame frame = queue.poll();
			if (frame == null) {
				return null;
			}
			queueSize.updateAndGet(current -> Math.max(0, current - 1));
			/*
			 * The frame waited too long and is already stale.
			 */
			if (now - frame.timestamp() > MAX_FRAME_AGE_MS) {
				droppedFrames.incrementAndGet();
				continue;
			}

			/*
			 * Send the oldest valid frame.
			 */
			return frame.message();
		}
	}

	/**
	 * Remove expired frames from the head of the queue.
	 */
	private void removeExpiredFrames(long now) {
		while (true) {
			Frame oldest = queue.peek();
			if (oldest == null) {
				return;
			}
			if (now - oldest.timestamp() <= MAX_FRAME_AGE_MS) {
				return;
			}
			Frame removed = queue.poll();
			if (removed != null) {
				queueSize.updateAndGet(current -> Math.max(0, current - 1));
				droppedFrames.incrementAndGet();
			}
		}
	}

	/**
	 * Remove all retained live frames.
	 */
	public void clear() {
		queue.clear();
		queueSize.set(0);
	}

	/**
	 * Number of dropped frames.
	 */
	public long getDroppedFrames() {
		return droppedFrames.get();
	}

	/**
	 * Current number of retained frames.
	 */
	public int size() {

		return queueSize.get();
	}

	/**
	 * Internal frame wrapper.
	 */
	private record Frame(TextMessage message, long timestamp) {
	}
}
