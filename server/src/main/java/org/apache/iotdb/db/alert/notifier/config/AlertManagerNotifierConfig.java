package org.apache.iotdb.db.alert.notifier.config;

public class AlertManagerNotifierConfig extends NotifierConfig {

    String host;

    public AlertManagerNotifierConfig(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

}
