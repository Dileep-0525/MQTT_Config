package com.dileep.mqtt.service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MqttGatewayService {

    @Value("${mqtt.broker-url}")
    private String brokerUrl;

    @Value("${mqtt.username}")
    private String username;

    @Value("${mqtt.password}")
    private String password;

    private final List<MqttMessageListener> listeners;

    /**
     * MQTT topics currently subscribed by gateway.
     */
    private final Set<String> subscribedTopics =
            ConcurrentHashMap.newKeySet();

    /**
     * Process MQTT messages asynchronously.
     */
    private final ExecutorService executor =
            Executors.newVirtualThreadPerTaskExecutor();

    private MqttClient mqttClient;

    @PostConstruct
    public void connect() {

        try {

            mqttClient =
                    new MqttClient(
                            brokerUrl,
                            "mqtt-gateway-" + UUID.randomUUID());

            MqttConnectOptions options =
                    new MqttConnectOptions();

            options.setAutomaticReconnect(true);
            options.setCleanSession(false);
            options.setUserName(username);
            options.setPassword(password.toCharArray());

            mqttClient.setCallback(new MqttCallbackExtended() {

                @Override
                public void connectComplete(
                        boolean reconnect,
                        String serverURI) {

                    if (!reconnect) {

                        log.info("MQTT Connected");

                        return;
                    }

                    log.info("MQTT Reconnected");

                    Set.copyOf(subscribedTopics)
                            .forEach(topic -> {

                                try {

                                    mqttClient.subscribe(topic, 1);

                                    log.info(
                                            "Restored Topic : {}",
                                            topic);

                                } catch (Exception ex) {

                                    log.error(
                                            "Restore Failed : {}",
                                            topic,
                                            ex);
                                }
                            });
                }

                @Override
                public void connectionLost(Throwable cause) {

                    log.error(
                            "MQTT Connection Lost",
                            cause);
                }

                @Override
                public void messageArrived(
                        String topic,
                        MqttMessage message) {

                    String payload =
                            new String(
                                    message.getPayload(),
                                    StandardCharsets.UTF_8);

                    executor.submit(() -> {

                        for (MqttMessageListener listener : listeners) {

                            try {

                                listener.onMessage(
                                        topic,
                                        payload);

                            } catch (Exception ex) {

                                log.error(
                                        "Listener failed",
                                        ex);
                            }
                        }
                    });
                }

                @Override
                public void deliveryComplete(
                        IMqttDeliveryToken token) {

                }

            });

            mqttClient.connect(options);

            log.info(
                    "MQTT Connected Successfully");

        } catch (Exception ex) {

            log.error(
                    "Failed connecting MQTT",
                    ex);
        }
    }

    /**
     * Subscribe MQTT topic.
     */
    public void subscribe(String topic) {

        if (topic == null || topic.isBlank()) {
            return;
        }

        try {

            if (!mqttClient.isConnected()) {

                log.warn(
                        "MQTT disconnected. Cannot subscribe.");

                return;
            }

            if (!subscribedTopics.add(topic)) {

                return;
            }

            mqttClient.subscribe(topic, 1);

            log.info(
                    "MQTT Subscribe : {}",
                    topic);

        } catch (Exception ex) {

            subscribedTopics.remove(topic);

            log.error(
                    "Subscribe failed : {}",
                    topic,
                    ex);
        }
    }

    /**
     * Unsubscribe MQTT topic.
     */
    public void unsubscribe(String topic) {

        if (topic == null) {
            return;
        }

        try {

            if (!mqttClient.isConnected()) {
                return;
            }

            if (!subscribedTopics.remove(topic)) {

                return;
            }

            mqttClient.unsubscribe(topic);

            log.info(
                    "MQTT Unsubscribe : {}",
                    topic);

        } catch (Exception ex) {

            log.error(
                    "Unsubscribe failed : {}",
                    topic,
                    ex);
        }
    }

    /**
     * Publish MQTT message.
     */
    public void publish(
            String topic,
            String payload) {

        try {

            if (!mqttClient.isConnected()) {

                log.warn(
                        "MQTT disconnected. Publish skipped.");

                return;
            }

            mqttClient.publish(
                    topic,
                    payload.getBytes(StandardCharsets.UTF_8),
                    1,
                    false);

        } catch (Exception ex) {

            log.error(
                    "Publish failed : {}",
                    topic,
                    ex);
        }
    }

    public boolean isConnected() {

        return mqttClient != null &&
                mqttClient.isConnected();
    }

    @PreDestroy
    public void disconnect() {

        try {

            executor.shutdownNow();

            if (mqttClient != null &&
                    mqttClient.isConnected()) {

                mqttClient.disconnect();

                mqttClient.close();

                log.info(
                        "MQTT Disconnected");
            }

        } catch (Exception ex) {

            log.error(
                    "Disconnect failed",
                    ex);
        }
    }
}


//package com.dileep.mqtt.service;
				//
				//import java.nio.charset.StandardCharsets;
				//import java.util.List;
				//import java.util.Set;
				//import java.util.UUID;
				//import java.util.concurrent.ConcurrentHashMap;
				//
				//import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
				//import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
				//import org.eclipse.paho.client.mqttv3.MqttClient;
				//import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
				//import org.eclipse.paho.client.mqttv3.MqttMessage;
				//import org.springframework.beans.factory.annotation.Value;
				//import org.springframework.stereotype.Service;
				//
				//import jakarta.annotation.PostConstruct;
				//import jakarta.annotation.PreDestroy;
				//import lombok.RequiredArgsConstructor;
				//import lombok.extern.slf4j.Slf4j;
				//
				//@Slf4j
				//@Service
				//@RequiredArgsConstructor
				//public class MqttGatewayService {
				//
				//	@Value("${mqtt.broker-url}")
				//	private String brokerUrl;
				//
				//	@Value("${mqtt.username}")
				//	private String username;
				//
				//	@Value("${mqtt.password}")
				//	private String password;
				//
				//	private final List<MqttMessageListener> listeners;
				//
				//	private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
				//
				////	private final SubscriptionRegistry subscriptionRegistry;
				//
				////	private final MqttGatewayService mqttGatewayService;
				//
				//	private MqttClient mqttClient;
				//
				//	/**
				//	 * Tracks already subscribed topics
				//	 */
				////	private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
				//
				//	@PostConstruct
				//	public void connect() {
				//		try {
				//			mqttClient = new MqttClient(brokerUrl, "mqtt-gateway-" + UUID.randomUUID());
				//			MqttConnectOptions options = new MqttConnectOptions();
				//			options.setUserName(username);
				//			options.setPassword(password.toCharArray());
				//			options.setAutomaticReconnect(true);
				//			// Later useful when reconnect happens
				//			options.setCleanSession(false);
				//			mqttClient.setCallback(new MqttCallbackExtended() {
				//				@Override
				//				public void connectComplete(boolean reconnect, String serverURI) {
				//					if (reconnect) {
				//						log.info("MQTT Reconnected");
				//						subscribedTopics.forEach(topic -> {
				//							try {
				//								mqttClient.subscribe(topic, 1);
				//								log.info("Restored MQTT topic: {}", topic);
				//							} catch (Exception ex) {
				//								log.error("Failed to restore topic {}", topic, ex);
				//							}
				//						});
				//					}
				//				}
				//
				//				@Override
				//				public void connectionLost(Throwable cause) {
				//					log.error("MQTT Connection Lost", cause);
				//				}
				//
				//				@Override
				//				public void messageArrived(String topic, MqttMessage message) {
				//					try {
				//						String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
				//						listeners.forEach(listener -> listener.onMessage(topic, payload));
				//					} catch (Exception ex) {
				//						log.error("Error processing MQTT message", ex);
				//					}
				//				}
				//
				//				@Override
				//				public void deliveryComplete(IMqttDeliveryToken token) {
				//					// No-op
				//				}
				//			});
				//			mqttClient.connect(options);
				//			log.info("MQTT Connected Successfully");
				//		} catch (Exception ex) {
				//			log.error("Failed to connect MQTT Broker", ex);
				//		}
				//	}
				//
				//	public void subscribe(String topic) {
				//		try {
				//			if (!subscribedTopics.contains(topic)) {
				//				mqttClient.subscribe(topic);
				//				subscribedTopics.add(topic);
				//			}
				//			if (subscribedTopics.contains(topic)) {
				//				return;
				//			}
				//			mqttClient.subscribe(topic, 1);
				//			subscribedTopics.add(topic);
				//			log.info("Subscribed to MQTT Topic: {}", topic);
				//		} catch (Exception ex) {
				//			log.error("Failed to subscribe topic {}", topic, ex);
				//		}
				//	}
				//
				//	public void unsubscribe(String topic) {
				//		try {
				//			if (!subscribedTopics.contains(topic)) {
				//				return;
				//			}
				//			mqttClient.unsubscribe(topic);
				//			subscribedTopics.remove(topic);
				//			log.info("MQTT unsubscribed : {} (Remaining Topics : {})", topic, subscribedTopics.size());
				//		} catch (Exception ex) {
				//			log.error("Failed to unsubscribe topic {}", topic, ex);
				//		}
				//	}
				//
				//	public boolean isConnected() {
				//		return mqttClient != null && mqttClient.isConnected();
				//	}
				//
				//	@PreDestroy
				//	public void disconnect() {
				//		try {
				//			if (mqttClient != null && mqttClient.isConnected()) {
				//				mqttClient.disconnect();
				//				mqttClient.close();
				//				log.info("MQTT Disconnected");
				//			}
				//		} catch (Exception ex) {
				//			log.error("Error while disconnecting MQTT", ex);
				//		}
				//	}
				//
				//	public void publish(String topic, String payload) {
				//		try {
				//			log.info("Publishing Topic : {}", topic);
				//			log.info("Publishing Payload : {}", payload);
				//			mqttClient.publish(topic, payload.getBytes(StandardCharsets.UTF_8), 1, false);
				//			log.info("Published to MQTT : {}", topic);
				//		} catch (Exception ex) {
				//			log.error("Failed to publish MQTT", ex);
				//		}
				//	}
				//}









//package com.dileep.service;
//
//import java.nio.charset.StandardCharsets;
//import java.util.List;
//import java.util.Set;
//import java.util.UUID;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
//import org.eclipse.paho.client.mqttv3.MqttCallback;
//import org.eclipse.paho.client.mqttv3.MqttClient;
//import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
//import org.eclipse.paho.client.mqttv3.MqttMessage;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import com.dileep.util.MqttMessageListener;
//
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
//
//@Slf4j
//@Service
//@RequiredArgsConstructor
//public class MqttGatewayService {
//
////	private final MqttProperties mqttProperties;
////
////	private final List<MqttMessageListener> listeners;
////	
////	private MqttClient mqttClient;
////
////	@PostConstruct
////	public void connect() {
////
////		try {
////			mqttClient = new MqttClient(mqttProperties.getBrokerUrl(), mqttProperties.getClientId());
////			MqttConnectOptions options = new MqttConnectOptions();
////			options.setAutomaticReconnect(true);
////			options.setCleanSession(false);
////			options.setUserName(mqttProperties.getUsername());
////			options.setPassword(mqttProperties.getPassword().toCharArray());
////			mqttClient.setCallback(new MqttCallback() {
////				@Override
////				public void connectionLost(Throwable cause) {
////					log.error("MQTT Connection Lost", cause);
////				}
////				@Override
////				public void messageArrived(String topic, MqttMessage message) {
////					try {
////						String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
////						listeners.forEach(listener -> listener.onMessage(topic, payload));
////                    } catch (Exception ex) {
////						log.error("Error processing MQTT message", ex);
////                    }
//	//// log.info("Topic: {}, Message: {}", topic,
//	/// new String(message.getPayload()));
////				}
////				@Override
////				public void deliveryComplete(IMqttDeliveryToken token) {
////				}
////			});
////			mqttClient.connect(options);
////			log.info("MQTT Connected Successfully");
////		} catch (Exception ex) {
////			log.error("Failed to connect MQTT", ex);
////		}
////	}
////
////	@PreDestroy
////	public void disconnect() {
////		try {
////			if (mqttClient != null && mqttClient.isConnected()) {
////				mqttClient.disconnect();
////			}
////		} catch (Exception ex) {
////			log.error("MQTT Disconnect Error", ex);
////		}
////	}
////
////	public void subscribe(String topic) {
////		try {
////			mqttClient.subscribe(topic);
////			log.info("Subscribed Topic: {}", topic);
////		} catch (Exception ex) {
////			log.error("Subscription Failed", ex);
////		}
////	}
//
//	@Value("${mqtt.broker-url}")
//	private String brokerUrl;
//
//	@Value("${mqtt.username}")
//	private String username;
//
//	@Value("${mqtt.password}")
//	private String password;
//
//	private final List<MqttMessageListener> listeners;
//
//	private MqttClient mqttClient;
//
//	/**
//	 * Tracks already subscribed topics
//	 */
//	private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
//
//	@PostConstruct
//	public void connect() {
//
//		try {
//
//			mqttClient = new MqttClient(brokerUrl, "mqtt-gateway-" + UUID.randomUUID());
//
//			MqttConnectOptions options = new MqttConnectOptions();
//
//			options.setUserName(username);
//			options.setPassword(password.toCharArray());
//
//			options.setAutomaticReconnect(true);
//			// Later useful when reconnect happens
//			options.setCleanSession(false);
//			mqttClient.setCallback(new MqttCallbackExtended() {
//				@Override
//				public void connectComplete(boolean reconnect, String serverURI) {
//					if (reconnect) {
//						log.info("MQTT Reconnected");
//						subscribedTopics.forEach(topic -> {
//							try {
//								mqttClient.subscribe(topic, 1);
//								log.info("Restored MQTT topic: {}", topic);
//							} catch (Exception ex) {
//								log.error("Failed to restore topic {}", topic, ex);
//							}
//						});
//					}
//				}
//
//				@Override
//				public void connectionLost(Throwable cause) {
//					log.error("MQTT Connection Lost", cause);
//				}
//
//				@Override
//				public void messageArrived(String topic, MqttMessage message) {
//					try {
//						String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
//						listeners.forEach(listener -> listener.onMessage(topic, payload));
//					} catch (Exception ex) {
//						log.error("Error processing MQTT message", ex);
//					}
//				}
//
//				@Override
//				public void deliveryComplete(IMqttDeliveryToken token) {
//
//					// No-op
//				}
//
//			});
//			mqttClient.connect(options);
//
//			log.info("MQTT Connected Successfully");
//		} catch (Exception ex) {
//			log.error("Failed to connect MQTT Broker", ex);
//		}
//	}
//
////	public void subscribe(String topic) {
////		try {
////			if (subscribedTopics.contains(topic)) {
////				return;
////			}
////			mqttClient.subscribe(topic, 1);
////			subscribedTopics.add(topic);
////			log.info("Subscribed to MQTT Topic: {}", topic);
////		} catch (Exception ex) {
////			log.error("Failed to subscribe topic {}", topic, ex);
////		}
////	}
//
//	public void unsubscribe(String topic) {
//		try {
//			if (!subscribedTopics.contains(topic)) {
//				return;
//			}
//			mqttClient.unsubscribe(topic);
//			subscribedTopics.remove(topic);
////			log.info("Unsubscribed MQTT Topic: {}", topic);
//			log.info("MQTT unsubscribed : {} (Remaining Topics : {})", topic, subscribedTopics.size());
//		} catch (Exception ex) {
//			log.error("Failed to unsubscribe topic {}", topic, ex);
//		}
//	}
//
//	public void subscribe(String topic) {
//
//		try {
//
//			if (topic == null || topic.isBlank()) {
//				return;
//			}
//
//			if (!mqttClient.isConnected()) {
//
//				throw new IllegalStateException("MQTT Client is not connected");
//			}
//
//			if (subscribedTopics.contains(topic)) {
//				return;
//			}
//
//			mqttClient.subscribe(topic, 1);
//
//			subscribedTopics.add(topic);
//
//			log.info("MQTT subscribed : {} (Total Topics : {})", topic, subscribedTopics.size());
//		} catch (Exception ex) {
//			log.error("Failed to subscribe topic {}", topic, ex);
//
//		}
//	}
//
//	public boolean isConnected() {
//		return mqttClient != null && mqttClient.isConnected();
//	}
//
//	@PreDestroy
//	public void disconnect() {
//		try {
//			if (mqttClient != null && mqttClient.isConnected()) {
//				mqttClient.disconnect();
//				mqttClient.close();
//				log.info("MQTT Disconnected");
//			}
//		} catch (Exception ex) {
//			log.error("Error while disconnecting MQTT", ex);
//		}
//	}
//}
