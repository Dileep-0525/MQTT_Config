//
//package com.dileep.mqtt.websocket;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.concurrent.atomic.AtomicReference;
//
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//
//import lombok.Getter;
//
//@Getter
//public class SessionContext {
//
//	/**
//	 * Actual websocket session.
//	 */
//	private final WebSocketSession session;
//
//	private volatile long lastPongTime = System.currentTimeMillis();
//	
//	/**
//	 * Latest LIVE frame.
//	 *
//	 * We never buffer live video. Always keep only the newest frame.
//	 */
//	private final AtomicReference<TextMessage> latestLiveFrame = new AtomicReference<>();
//	
//	private final LiveFrameBuffer liveBuffer = new LiveFrameBuffer();
//	/**
//	 * Last heartbeat from browser.
//	 */
//	private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());
//
//	/**
//	 * Playback must preserve ordering.
//	 */
//	private final BlockingQueue<TextMessage> playbackQueue = new LinkedBlockingQueue<>(100);
//
//	/**
//	 * Events are small.
//	 */
//	private final BlockingQueue<TextMessage> controlQueue = new LinkedBlockingQueue<>(200);
//
//	/**
//	 * Sender thread running flag.
//	 */
//	private final AtomicBoolean running = new AtomicBoolean(true);
//
//	/**
//	 * Sender thread.
//	 */
//	private Thread senderThread;
//
//	public SessionContext(WebSocketSession session) {
//		this.session = session;
//	}
//
//	/**
//	 * Replace previous LIVE frame. Never queue.
//	 */
//	public void enqueueLive(TextMessage message) {
//
//		latestLiveFrame.set(message);
//
//	}
//
//	/**
//	 * Sender consumes latest frame.
//	 */
//	public TextMessage pollLive() {
//
//		return latestLiveFrame.getAndSet(null);
//
//	}
//
//	/**
//	 * Playback queue.
//	 */
//	public boolean enqueuePlayback(TextMessage message) {
//
//		return playbackQueue.offer(message);
//
//	}
//
//	/**
//	 * Event queue.
//	 */
//	public boolean enqueueControl(TextMessage message) {
//
//		return controlQueue.offer(message);
//
//	}
//
//	/**
//	 * Cleanup.
//	 */
//	public void stop() {
//
//		running.set(false);
//
//		if (senderThread != null) {
//			senderThread.interrupt();
//		}
//
//		latestLiveFrame.set(null);
//
//		playbackQueue.clear();
//
//		controlQueue.clear();
//	}
//
//	public void setSenderThread(Thread senderThread) {
//
//		this.senderThread = senderThread;
//
//	}
//
//	public void heartbeat() {
//
//		lastHeartbeat.set(System.currentTimeMillis());
//
//	}
//
//	public long getLastHeartbeat() {
//
//		return lastHeartbeat.get();
//
//	}
//
//	public long getLastPongTime() {
//	    return lastPongTime;
//	}
//
//	public void setLastPongTime(long lastPongTime) {
//	    this.lastPongTime = lastPongTime;
//	}
//	
//}

//package com.dileep.mqtt.websocket;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.springframework.web.socket.TextMessage;
//import org.springframework.web.socket.WebSocketSession;
//
//import lombok.Getter;
//
//@Getter
//public class SessionContext {
//
//    /**
//     * Actual websocket session.
//     */
//    private final WebSocketSession session;
//
//    /**
//     * Adaptive live frame buffer.
//     */
//    private final LiveFrameBuffer liveBuffer = new LiveFrameBuffer();
//
//    /**
//     * Playback frames.
//     */
//    private final BlockingQueue<TextMessage> playbackQueue =
//            new LinkedBlockingQueue<>(100);
//
//    /**
//     * Control/Event messages.
//     */
//    private final BlockingQueue<TextMessage> controlQueue =
//            new LinkedBlockingQueue<>(200);
//
//    /**
//     * Sender thread running flag.
//     */
//    private final AtomicBoolean running =
//            new AtomicBoolean(true);
//
//    /**
//     * Last heartbeat received.
//     */
//    private final AtomicLong lastHeartbeat =
//            new AtomicLong(System.currentTimeMillis());
//
//    /**
//     * Last browser PONG.
//     */
//    private volatile long lastPongTime =
//            System.currentTimeMillis();
//
//    /**
//     * Sender thread.
//     */
//    private Thread senderThread;
//
//    public SessionContext(WebSocketSession session) {
//        this.session = session;
//    }
//
//    /**
//     * Live frame.
//     */
//    public void enqueueLive(TextMessage message) {
//        liveBuffer.offer(message);
//    }
//
//    /**
//     * Sender consumes live frame.
//     */
//    public TextMessage pollLive() {
//        return liveBuffer.poll();
//    }
//
//    /**
//     * Current live queue size.
//     */
//    public int getLiveQueueSize() {
//        return liveBuffer.size();
//    }
//
//    /**
//     * Current adaptive capacity.
//     */
//    public int getLiveCapacity() {
//        return liveBuffer.capacity();
//    }
//
//    /**
//     * Playback queue.
//     */
//    public boolean enqueuePlayback(TextMessage message) {
//        return playbackQueue.offer(message);
//    }
//
//    /**
//     * Control/Event queue.
//     */
//    public boolean enqueueControl(TextMessage message) {
//        return controlQueue.offer(message);
//    }
//
//    /**
//     * Heartbeat received.
//     */
//    public void heartbeat() {
//        lastHeartbeat.set(System.currentTimeMillis());
//    }
//
//    /**
//     * Cleanup.
//     */
//    public void stop() {
//
//        running.set(false);
//
//        if (senderThread != null) {
//            senderThread.interrupt();
//        }
//
//        liveBuffer.clear();
//
//        playbackQueue.clear();
//
//        controlQueue.clear();
//    }
//
//    public void setSenderThread(Thread senderThread) {
//        this.senderThread = senderThread;
//    }
//
//    public long getLastHeartbeat() {
//        return lastHeartbeat.get();
//    }
//
//    public long getLastPongTime() {
//        return lastPongTime;
//    }
//
//    public void setLastPongTime(long lastPongTime) {
//        this.lastPongTime = lastPongTime;
//    }
//}

package com.dileep.mqtt.websocket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.Getter;

@Getter
public class SessionContext {

	/**
	 * Actual websocket session.
	 */
	private final WebSocketSession session;

	/**
	 * Wakes sender thread when new message arrives.
	 */
	private final Semaphore signal = new Semaphore(0);

	/**
	 * Consecutive write timeouts.
	 */
	private final AtomicInteger writeTimeouts = new AtomicInteger();
	
	/**
	 * Live video buffer.
	 */
	private final LiveFrameBuffer liveBuffer = new LiveFrameBuffer();

	/**
	 * Playback queue.
	 */
	private final BlockingQueue<TextMessage> playbackQueue = new LinkedBlockingQueue<>(100);

	/**
	 * Control/Event queue.
	 */
	private final BlockingQueue<TextMessage> controlQueue = new LinkedBlockingQueue<>(200);

	/**
	 * Sender thread running flag.
	 */
	private final AtomicBoolean running = new AtomicBoolean(true);

	/**
	 * Last heartbeat received.
	 */
	private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());

	/**
	 * Last browser PONG.
	 */
	private volatile long lastPongTime = System.currentTimeMillis();

	/**
	 * Sender thread.
	 */
	private Thread senderThread;

	public SessionContext(WebSocketSession session) {
		this.session = session;
	}

	/**
	 * Queue a live frame.
	 */
	public void enqueueLive(TextMessage message) {
		liveBuffer.offer(message);
	    signal.release();
	}

	/**
	 * Returns the newest valid frame.
	 */
	public TextMessage pollLive() {
		return liveBuffer.poll();
	}

	/**
	 * Current live buffer size.
	 */
	public int getLiveQueueSize() {
		return liveBuffer.size();
	}

	/**
	 * Number of dropped live frames.
	 */
	public long getDroppedLiveFrames() {
		return liveBuffer.getDroppedFrames();
	}

	/**
	 * Playback queue.
	 */
	public boolean enqueuePlayback(TextMessage message) {
	    boolean added = playbackQueue.offer(message);
	    if (added) {
	        signal.release();
	    }
	    return added;
	}

	/**
	 * Control/Event queue.
	 */
	public boolean enqueueControl(TextMessage message) {
	    boolean added = controlQueue.offer(message);
	    if (added) {
	        signal.release();
	    }
	    return added;
	}

	/**
	 * Browser heartbeat received.
	 */
	public void heartbeat() {
		lastHeartbeat.set(System.currentTimeMillis());
	}

	/**
	 * Cleanup.
	 */
	public void stop() {

		running.set(false);

		if (senderThread != null) {
			senderThread.interrupt();
		}

		liveBuffer.clear();

		playbackQueue.clear();

		controlQueue.clear();
	}

	public void setSenderThread(Thread senderThread) {
		this.senderThread = senderThread;
	}

	public long getLastHeartbeat() {
		return lastHeartbeat.get();
	}

	public long getLastPongTime() {
		return lastPongTime;
	}

	public void setLastPongTime(long lastPongTime) {
		this.lastPongTime = lastPongTime;
	}
}
