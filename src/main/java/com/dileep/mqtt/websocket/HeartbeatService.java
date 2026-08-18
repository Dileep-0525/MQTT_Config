package com.dileep.mqtt.websocket;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.WebSocketSession;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class HeartbeatService {

    private final SessionManager sessionManager;

    private final SubscriptionService subscriptionService;

    /**
     * Every 30 seconds
     */
    @Scheduled(fixedDelay = 30000)
    public void heartbeat() {

        for (SessionContext context : sessionManager.getAllContexts()) {

            WebSocketSession session = context.getSession();

            try {

                if (!session.isOpen()) {

                    cleanup(session);

                    continue;
                }

                session.sendMessage(new PingMessage());
                

            } catch (Exception ex) {

                log.warn("Heartbeat failed : {}", session.getId());

                cleanup(session);
            }
        }
    }

    private void cleanup(WebSocketSession session) {

        try {

            subscriptionService.removeSession(session);

            sessionManager.unregister(session);

            if (session.isOpen()) {

                session.close(CloseStatus.SESSION_NOT_RELIABLE);
            }

            log.info("Dead websocket removed : {}", session.getId());

        } catch (Exception ex) {

            log.error("Cleanup failed : {}", session.getId(), ex);
        }
    }

}