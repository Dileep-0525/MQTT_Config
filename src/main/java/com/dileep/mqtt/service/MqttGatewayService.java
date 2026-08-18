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
	private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();

	/**
	 * Process MQTT messages asynchronously.
	 */
	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private MqttClient mqttClient;

	@PostConstruct
	public void connect() {

		try {

			mqttClient = new MqttClient(brokerUrl, "mqtt-gateway-" + UUID.randomUUID());

			MqttConnectOptions options = new MqttConnectOptions();

			options.setAutomaticReconnect(true);
			options.setCleanSession(false);
			options.setUserName(username);
			options.setPassword(password.toCharArray());

			mqttClient.setCallback(new MqttCallbackExtended() {

				@Override
				public void connectComplete(boolean reconnect, String serverURI) {

					if (!reconnect) {

						log.info("MQTT Connected");

						return;
					}

					log.info("MQTT Reconnected");

					Set.copyOf(subscribedTopics).forEach(topic -> {

						try {

							mqttClient.subscribe(topic, 1);

							log.info("Restored Topic : {}", topic);

						} catch (Exception ex) {

							log.error("Restore Failed : {}", topic, ex);
						}
					});
				}

				@Override
				public void connectionLost(Throwable cause) {

					log.error("MQTT Connection Lost", cause);
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) {

//					String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
				    String payload = message.toString();

					executor.submit(() -> {

						for (MqttMessageListener listener : listeners) {

							try {

								listener.onMessage(topic, payload);
//								listener.onMessage(topic, message.toString());
							} catch (Exception ex) {
								log.error("Listener failed", ex);
							}
						}
					});
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {

				}

			});

			mqttClient.connect(options);

			log.info("MQTT Connected Successfully");

		} catch (Exception ex) {

			log.error("Failed connecting MQTT", ex);
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

				log.warn("MQTT disconnected. Cannot subscribe.");

				return;
			}

			if (!subscribedTopics.add(topic)) {

				return;
			}

			mqttClient.subscribe(topic, 1);

			log.info("MQTT Subscribe : {}", topic);

		} catch (Exception ex) {

			subscribedTopics.remove(topic);

			log.error("Subscribe failed : {}", topic, ex);
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

			log.info("MQTT Unsubscribe : {}", topic);

		} catch (Exception ex) {

			log.error("Unsubscribe failed : {}", topic, ex);
		}
	}

	/**
	 * Publish MQTT message.
	 */
	public void publish(String topic, String payload) {

		try {

			if (!mqttClient.isConnected()) {

				log.warn("MQTT disconnected. Publish skipped.");

				return;
			}

			mqttClient.publish(topic, payload.getBytes(StandardCharsets.UTF_8), 1, false);

		} catch (Exception ex) {

			log.error("Publish failed : {}", topic, ex);
		}
	}

	public boolean isConnected() {

		return mqttClient != null && mqttClient.isConnected();
	}

	@PreDestroy
	public void disconnect() {
		try {
			executor.shutdownNow();
			if (mqttClient != null && mqttClient.isConnected()) {
				mqttClient.disconnect();
				mqttClient.close();
				log.info("MQTT Disconnected");
			}
		} catch (Exception ex) {
			log.error("Disconnect failed", ex);
		}
	}
}
