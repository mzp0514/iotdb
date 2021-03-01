package org.apache.iotdb.db.alert.notifier.config;

import java.time.Duration;

public class NotifierConfig {

    Duration sendInterval; // 同一 alertname 同一 severity 的告警发送的时间间隔

    // int sendCount; // 同一 alertname 的告警发送的最大次数

    public NotifierConfig() {}

    public NotifierConfig(Duration sendInterval) {
        this.sendInterval = sendInterval;
    }

    public Duration getSendInterval() {
        return sendInterval;
    }

}

