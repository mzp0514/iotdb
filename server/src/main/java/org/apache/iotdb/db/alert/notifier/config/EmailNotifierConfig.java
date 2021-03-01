package org.apache.iotdb.db.alert.notifier.config;
import org.simplejavamail.api.mailer.config.TransportStrategy;

import java.time.Duration;
import java.util.List;

public class EmailNotifierConfig extends NotifierConfig {

    private final TransportStrategy strategy;
    private final String smtpHost;
    private final Integer smtpPort;
    private final String username;
    private final String password;
    private final List<String> receivers;

    public EmailNotifierConfig(Duration sendInterval, String transportStrategy,
                               String smtpHost, Integer smtpPort, String username, String password, List<String> receivers) {
        super(sendInterval);
        transportStrategy = transportStrategy.toUpperCase();
        switch (transportStrategy) {
            case "SMTP":
                strategy = TransportStrategy.SMTP;
                break;
            case "SMTPS":
                strategy = TransportStrategy.SMTPS;
                break;
            case "SMTP_TLS":
                strategy = TransportStrategy.SMTP_TLS;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported SMTP Transport Strategy.");
        }
        this.smtpHost = smtpHost;
        this.smtpPort = smtpPort;
        this.username = username;
        this.password = password;
        this.receivers = receivers;
    }

    public TransportStrategy getStrategy() {
        return strategy;
    }

    public String getSmtpHost() {
        return smtpHost;
    }

    public Integer getSmtpPort() {
        return smtpPort;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public List<String> getReceivers() {
        return receivers;
    }
}
