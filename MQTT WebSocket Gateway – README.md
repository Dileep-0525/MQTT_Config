# MQTT WebSocket Gateway

A Spring Boot based MQTT-to-WebSocket gateway that allows browser clients to authenticate using JWT, subscribe to multiple MQTT topics, receive live camera/inference streams, playback messages, and event notifications through a WebSocket connection.

The application maintains MQTT subscriptions centrally and distributes incoming MQTT messages to the appropriate WebSocket sessions.

---

## 1. Architecture

```text
                    ┌──────────────────────┐
                    │     MQTT Broker      │
                    │      Mosquitto       │
                    └──────────┬───────────┘
                               │
                               │ MQTT
                               ▼
                    ┌──────────────────────┐
                    │ MqttGatewayService   │
                    │                      │
                    │ - Connect            │
                    │ - Subscribe          │
                    │ - Unsubscribe        │
                    │ - Receive messages   │
                    │ - Publish            │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ WebSocketMqttListener│
                    │                      │
                    │ Topic classification │
                    │ Message fan-out      │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┼──────────────┐
                 │             │              │
                 ▼             ▼              ▼
          Live Frame       Playback        Events
            Buffer          Queue           Queue
                 │             │              │
                 └─────────────┼──────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    SessionSender     │
                    │                      │
                    │ Per-session sender   │
                    │ Write handling       │
                    └──────────┬───────────┘
                               │
                               │ WebSocket
                               ▼
                    ┌──────────────────────┐
                    │      Browser         │
                    │   / Postman Client   │
                    └──────────────────────┘
```

---

# 2. Main Features

- MQTT broker integration using Eclipse Paho.
- Dynamic MQTT topic subscription.
- Multiple WebSocket topic subscriptions per client.
- Centralized MQTT subscription management.
- JWT-based WebSocket authentication.
- Per-WebSocket-session message management.
- Live camera/inference stream handling.
- Playback message handling.
- Event/control message handling.
- Live frame buffering with timestamp-based expiration.
- Per-session sender virtual thread.
- WebSocket write timeout handling.
- Browser/client disconnect detection.
- WebSocket heartbeat using Ping/Pong.
- Automatic MQTT reconnection.
- MQTT topic restoration after reconnect.
- Automatic cleanup of disconnected WebSocket sessions.

---

# 3. Technology Stack

| Technology | Purpose |
|---|---|
| Java | Application development |
| Spring Boot | Application framework |
| Spring WebSocket | WebSocket communication |
| Eclipse Paho MQTT | MQTT client |
| JWT | WebSocket authentication |
| Lombok | Boilerplate reduction |
| Mosquitto | MQTT broker |
| Virtual Threads | Per-session message sender |

---

# 4. MQTT Topic Structure

The application currently identifies topics based on their naming convention.

### Live inference

```text
live/{cameraId}/{streamId}/vision-inference
```

Example:

```text
live/1/396/vision-inference
```

These messages are treated as live-stream frames.

---

### Live events

```text
live/{cameraId}/{streamId}/events
```

These messages are treated as event/control messages.

---

### Playback

```text
hist/{...}
```

These messages are treated as playback messages.

---

# 5. Topic Types

The application uses `TopicType` to classify MQTT messages.

```java
public enum TopicType {

    LIVE,
    PLAYBACK,
    EVENT
}
```

Topic classification is performed by `WebSocketMqttListener`.

```text
live/*/vision-inference → LIVE
live/*/events           → EVENT
hist/*                  → PLAYBACK
```

Unknown topics are ignored and logged.

---

# 6. MQTT Gateway

## MqttGatewayService

`MqttGatewayService` is responsible for communication with the MQTT broker.

Responsibilities:

- Establish MQTT connection.
- Configure authentication.
- Enable automatic reconnect.
- Subscribe to topics.
- Unsubscribe from topics.
- Receive MQTT messages.
- Publish MQTT messages.
- Restore subscriptions after reconnect.
- Disconnect during application shutdown.

### Connection configuration

The MQTT client uses:

```java
options.setAutomaticReconnect(true);
options.setCleanSession(false);
options.setUserName(username);
options.setPassword(password.toCharArray());
```

---

# 7. MQTT Message Processing

When an MQTT message arrives:

```java
@Override
public void messageArrived(String topic, MqttMessage message) {

    String payload =
            new String(message.getPayload(), StandardCharsets.UTF_8);

    for (MqttMessageListener listener : listeners) {

        listener.onMessage(topic, payload);
    }
}
```

The MQTT payload is converted directly into a UTF-8 `String`.

The application does not deserialize the MQTT payload into a Java object before forwarding it.

---

# 8. WebSocket Message Forwarding

`WebSocketMqttListener` receives MQTT messages.

It performs:

1. Topic classification.
2. WebSocket message construction.
3. Subscriber lookup.
4. Fan-out to all subscribed sessions.
5. Queue selection based on topic type.

Example:

```java
switch (type) {

    case LIVE ->
        sessionManager.enqueueLive(session, message);

    case PLAYBACK ->
        sessionManager.enqueuePlayback(session, message);

    case EVENT ->
        sessionManager.enqueueControl(session, message);
}
```

---

# 9. WebSocket Subscription Architecture

The application supports multiple MQTT subscriptions on a single WebSocket connection.

For example, one WebSocket session can subscribe to:

```text
live/1/395/vision-inference
live/1/396/vision-inference
live/1/397/vision-inference
live/1/395/events
hist/1/395
```

The `SubscriptionRegistry` maintains two mappings.

### Topic → Sessions

```text
Topic
  ↓
WebSocket Sessions
```

### Session → Topics

```text
WebSocket Session
  ↓
Subscribed Topics
```

This allows the application to efficiently determine:

- Which sessions are subscribed to a topic.
- Which topics belong to a WebSocket session.

---

# 10. Subscription Flow

The browser sends:

```json
{
  "action": "SUBSCRIBE",
  "topic": "live/1/396/vision-inference"
}
```

The flow is:

```text
Browser
   ↓
CameraWebSocketHandler
   ↓
SubscriptionService
   ↓
SubscriptionRegistry
   ↓
MqttGatewayService
   ↓
MQTT Broker
```

The MQTT broker subscription is created only when the topic gets its first WebSocket subscriber.

---

# 11. Shared MQTT Subscription

If multiple WebSocket clients subscribe to the same MQTT topic:

```text
Topic:
live/1/396/vision-inference

        ┌───────────────┐
        │ MQTT Broker   │
        └───────┬───────┘
                │
                ▼
        MqttGatewayService
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
   WebSocket 1      WebSocket 2
```

The gateway maintains a single MQTT subscription instead of creating one MQTT subscription for every browser session.

This reduces unnecessary MQTT connections/subscriptions.

---

# 12. Unsubscription

When a WebSocket session unsubscribes:

```text
WebSocket
   ↓
SubscriptionService
   ↓
SubscriptionRegistry
```

If other sessions are still subscribed to the same topic:

```text
MQTT subscription remains active
```

If the session was the last subscriber:

```text
SubscriptionRegistry
       ↓
No subscribers remain
       ↓
MqttGatewayService.unsubscribe()
```

The MQTT topic is then unsubscribed.

---

# 13. WebSocket Session Management

`SessionManager` maintains:

```text
WebSocketSession
        ↓
SessionContext
```

Every WebSocket connection gets its own `SessionContext`.

When a session is registered:

```java
SessionContext context =
        new SessionContext(session);

sessions.put(session, context);

sessionSender.start(context);
```

A dedicated virtual sender thread is created for the session.

---

# 14. SessionContext

`SessionContext` contains all state associated with a WebSocket connection.

It maintains:

```text
Session
   │
   ├── LiveFrameBuffer
   │
   ├── Playback Queue
   │
   ├── Control/Event Queue
   │
   ├── Semaphore
   │
   ├── Write Timeout Counter
   │
   ├── Heartbeat information
   │
   └── Sender Thread
```

---

# 15. Live Frame Buffer

Live frames are stored temporarily in:

```java
LiveFrameBuffer
```

The buffer protects the application from unlimited live-frame accumulation.

Current configuration:

```java
MAX_FRAME_AGE_MS = 700;
MAX_QUEUE_SIZE = 10;
```

A frame contains:

```java
record Frame(
    TextMessage message,
    long timestamp
)
```

The timestamp represents the time at which the frame entered the buffer.

---

# 16. Timestamp-Based Frame Expiration

When a frame becomes older than the configured maximum age:

```text
MQTT frame
    ↓
LiveFrameBuffer
    ↓
700 ms exceeded
    ↓
Frame discarded
```

This prevents a slow WebSocket client from continuously receiving old live frames.

For live video, an old frame is generally less useful than a newer frame.

---

# 17. Queue Types

The application separates different message categories.

### Live

```java
LiveFrameBuffer
```

Used for camera/inference streaming.

### Playback

```java
LinkedBlockingQueue<>(100)
```

Used for playback messages.

### Control/Event

```java
LinkedBlockingQueue<>(200)
```

Used for events and control messages.

---

# 18. SessionSender

Each WebSocket session has a dedicated virtual sender.

```java
Thread.ofVirtual().start(...)
```

The sender checks the queues according to priority:

```text
1. Live
2. Playback
3. Control/Event
```

This allows live video traffic to be processed with higher priority.

---

# 19. WebSocket Write Handling

The sender performs:

```java
session.sendMessage(message);
```

After a successful send:

```java
context.getWriteTimeouts().set(0);
```

This resets the consecutive write-timeout counter.

---

# 20. Write Timeout Handling

A slow browser can cause WebSocket write timeouts.

The application tracks consecutive failures:

```java
MAX_WRITE_TIMEOUTS = 5;
```

The sequence is:

```text
Write timeout
     ↓
Increment counter
     ↓
Retry
     ↓
Successful write
     ↓
Counter = 0
```

If consecutive write timeouts reach the configured limit:

```text
Too many write timeouts
        ↓
Sender stops
        ↓
Session cleanup
```

---

# 21. Client Disconnect Detection

The sender checks for common disconnect conditions such as:

```text
connection reset
broken pipe
transformer has been closed
deflater has been closed
closed
```

When the client is disconnected:

```text
Sender
  ↓
Detect disconnect
  ↓
Stop sender
  ↓
SessionContext.stop()
```

---

# 22. WebSocket Heartbeat

The application implements a WebSocket heartbeat mechanism.

`HeartbeatService` executes periodically:

```java
@Scheduled(fixedDelay = 30000)
```

The interval is:

```text
30 seconds
```

For every active WebSocket session:

```java
session.sendMessage(new PingMessage());
```

The browser/client responds with a WebSocket Pong.

---

# 23. Pong Handling

`CameraWebSocketHandler` receives the Pong message:

```java
@Override
protected void handlePongMessage(
        WebSocketSession session,
        PongMessage message) {

    SessionContext context =
            sessionManager.getContext(session);

    if (context != null) {

        context.setLastPongTime(
                System.currentTimeMillis()
        );
    }
}
```

The last Pong timestamp is maintained inside `SessionContext`.

---

# 24. Heartbeat Flow

```text
HeartbeatService
       │
       │ every 30 seconds
       ▼
 WebSocket Ping
       │
       ▼
    Browser
       │
       │ Pong
       ▼
CameraWebSocketHandler
       │
       ▼
SessionContext
       │
       ▼
lastPongTime updated
```

The heartbeat helps detect unhealthy WebSocket connections.

---

# 25. Heartbeat Failure Cleanup

If sending the heartbeat itself fails:

```text
Heartbeat
   ↓
Exception
   ↓
cleanup()
```

Cleanup performs:

```text
SubscriptionService.removeSession()
        ↓
SessionManager.unregister()
        ↓
SessionContext.stop()
        ↓
WebSocket close
```

The session is closed using:

```java
CloseStatus.SESSION_NOT_RELIABLE
```

---

# 26. WebSocket Authentication

The WebSocket client first authenticates using:

```json
{
  "action": "AUTH",
  "token": "<JWT>"
}
```

The handler validates the token using `JwtUtil`.

Successful authentication:

```text
AUTH_SUCCESS
```

Invalid authentication:

```text
AUTH_FAILED
```

The connection is closed after authentication failure.

---

# 27. Subscription Authentication

A client cannot subscribe before authentication.

If the client attempts:

```json
{
  "action": "SUBSCRIBE",
  "topic": "live/1/396/vision-inference"
}
```

without authentication, the application responds:

```text
NOT_AUTHENTICATED
```

and closes the WebSocket connection.

---

# 28. Playback

Playback requests are handled separately from live streaming.

The WebSocket client sends a playback request.

The handler converts the request payload to JSON and publishes it through:

```java
subscriptionService.publish(...)
```

The flow is:

```text
Browser
   ↓
CameraWebSocketHandler
   ↓
SubscriptionService
   ↓
MqttGatewayService
   ↓
MQTT Broker
```

Playback responses are classified as:

```text
PLAYBACK
```

and placed into the playback queue.

---

# 29. Complete Message Flow

For a live camera frame:

```text
Camera
   ↓
MQTT Broker
   ↓
MqttGatewayService
   ↓
MqttMessageListener
   ↓
WebSocketMqttListener
   ↓
SubscriptionRegistry
   ↓
SessionManager
   ↓
SessionContext
   ↓
LiveFrameBuffer
   ↓
SessionSender
   ↓
ConcurrentWebSocketSessionDecorator
   ↓
WebSocket
   ↓
Browser
```

---

# 30. Session Cleanup

A session can be cleaned up from multiple paths:

### Normal WebSocket close

```text
afterConnectionClosed()
```

### Heartbeat failure

```text
HeartbeatService.cleanup()
```

### Sender failure

```text
SessionSender
   ↓
context.stop()
```

### Client disconnect

```text
connection reset / broken pipe
```

During cleanup:

```text
SubscriptionRegistry
       ↓
Remove session from all topics
       ↓
Unsubscribe MQTT topics with no remaining subscribers
       ↓
SessionManager.unregister()
       ↓
SessionContext.stop()
```

---

# 31. Resource Management

The application uses:

### ConcurrentHashMap

For thread-safe session and subscription registries.

### ConcurrentLinkedQueue

For live frame buffering.

### LinkedBlockingQueue

For playback and control messages.

### AtomicInteger

For write-timeout tracking and queue size tracking.

### AtomicLong

For heartbeat and dropped-frame statistics.

### Semaphore

For waking the sender thread when messages are available.

### Virtual Threads

For lightweight per-WebSocket sender execution.

---

# 32. Important Design Principle

The application intentionally separates:

```text
MQTT receiving
```

from:

```text
WebSocket sending
```

MQTT reception should not directly perform a potentially slow WebSocket write.

Instead:

```text
MQTT
  ↓
Queue/Buffer
  ↓
WebSocket Sender
```

This prevents a slow browser from blocking MQTT message processing.

---

# 33. Current Live Streaming Strategy

The live stream is treated differently from normal application messages.

The application allows live frames to be dropped when they become stale.

Conceptually:

```text
Frame 100
Frame 101
Frame 102
Frame 103
       ↓
Browser becomes slow
       ↓
Old frames become stale
       ↓
Discard old frames
       ↓
Continue with newer frames
```

The objective is to maintain low latency rather than guarantee delivery of every video frame.

---

# 34. Configuration

Example MQTT configuration:

```properties
mqtt.broker-url=tcp://localhost:1883
mqtt.username=<username>
mqtt.password=<password>
```

The actual broker URL, username, and password should be supplied through environment-specific configuration.

---

# 35. Example WebSocket Requests

## Authenticate

```json
{
  "action": "AUTH",
  "token": "<JWT>"
}
```

---

## Subscribe to live stream

```json
{
  "action": "SUBSCRIBE",
  "topic": "live/1/396/vision-inference"
}
```

---

## Subscribe to another camera

```json
{
  "action": "SUBSCRIBE",
  "topic": "live/1/397/vision-inference"
}
```

Multiple subscriptions can be maintained on the same WebSocket connection.

---

## Playback

```json
{
  "action": "PLAYBACK",
  "topic": "hist/1/396",
  "payload": {}
}
```

The exact playback payload depends on the MQTT playback API.

---

# 36. Important Classes

```text
com.dileep.mqtt
│
├── service
│   ├── MqttGatewayService
│   └── MqttMessageListener
│
├── websocket
│   ├── CameraWebSocketHandler
│   ├── SessionManager
│   ├── SessionContext
│   ├── SessionSender
│   ├── LiveFrameBuffer
│   ├── SubscriptionRegistry
│   ├── SubscriptionService
│   ├── WebSocketMqttListener
│   └── HeartbeatService
│
├── dto
│   ├── WebSocketRequest
│   └── TopicType
│
└── util
    └── JwtUtil
```

---

# 37. Responsibilities

| Class | Responsibility |
|---|---|
| `MqttGatewayService` | MQTT connection, subscribe, unsubscribe, publish, receive |
| `WebSocketMqttListener` | MQTT → WebSocket message forwarding |
| `SubscriptionRegistry` | Topic/session relationship management |
| `SubscriptionService` | Subscription lifecycle coordination |
| `CameraWebSocketHandler` | WebSocket connection, authentication and client requests |
| `SessionManager` | WebSocket session context management |
| `SessionContext` | Per-session state and queues |
| `SessionSender` | Sends queued messages to WebSocket |
| `LiveFrameBuffer` | Temporary live frame buffering and stale-frame removal |
| `HeartbeatService` | WebSocket Ping heartbeat |
| `JwtUtil` | JWT validation |

---

# 38. Shutdown

During application shutdown:

```java
@PreDestroy
public void disconnect()
```

The MQTT client is disconnected and closed.

This prevents MQTT resources from remaining open after application termination.

---

# 39. Current Architecture Summary

The application follows:

```text
                   MQTT
                    │
                    ▼
          MqttGatewayService
                    │
                    ▼
        WebSocketMqttListener
                    │
             Topic Routing
                    │
       ┌────────────┼────────────┐
       │            │            │
       ▼            ▼            ▼
     LIVE       PLAYBACK       EVENT
       │            │            │
       ▼            ▼            ▼
 LiveBuffer    PlaybackQueue  ControlQueue
       │            │            │
       └────────────┼────────────┘
                    ▼
             SessionSender
                    │
                    ▼
               WebSocket
                    │
                    ▼
                 Client

Heartbeat:

HeartbeatService
       │
       ▼
      Ping
       │
       ▼
     Client
       │
       ▼
      Pong
       │
       ▼
CameraWebSocketHandler
```

---

# 40. Design Goals

The architecture is designed around the following goals:

- Support multiple cameras.
- Support multiple WebSocket clients.
- Support multiple MQTT topic subscriptions per WebSocket session.
- Prevent slow WebSocket clients from blocking MQTT reception.
- Keep live-stream latency low.
- Drop stale live frames when necessary.
- Detect disconnected clients.
- Clean up MQTT subscriptions when no WebSocket clients require them.
- Maintain a heartbeat for WebSocket connection health.
- Keep live, playback, and event messages logically separated.
- Use lightweight virtual threads for per-session sending.