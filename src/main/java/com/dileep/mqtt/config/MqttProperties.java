package com.dileep.mqtt.config;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@ConfigurationProperties(prefix = "mqtt")
@Data
public class MqttProperties {

    private String brokerUrl;
    private String clientId;
    private String username;
    private String password;

}
