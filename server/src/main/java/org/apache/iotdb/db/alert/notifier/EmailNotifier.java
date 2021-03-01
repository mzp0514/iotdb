package org.apache.iotdb.db.alert.notifier;


import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.iotdb.db.alert.notifier.config.EmailNotifierConfig;
import org.apache.iotdb.db.alert.notifier.config.NotifierConfig;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.simplejavamail.api.email.EmailPopulatingBuilder;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;

public class EmailNotifier extends Notifier {

    EmailNotifierConfig emailConfig;

    ConcurrentHashMap<String, Instant> lastSentTimestamp = new ConcurrentHashMap<>();

    private final Mailer mailer;

    public EmailNotifier(String notifierId, NotifierConfig config) {

        super();

        emailConfig = (EmailNotifierConfig) config;

        mailer = MailerBuilder
                .withTransportStrategy(emailConfig.getStrategy())
                .withSMTPServer(
                        emailConfig.getSmtpHost(), emailConfig.getSmtpPort(),
                        emailConfig.getUsername(), emailConfig.getPassword())
                .withProperty("mail.smtp.sendpartial", true)
                .clearEmailAddressCriteria()
                .buildMailer();

        this.pool = IoTDBThreadPoolFactory.newCachedThreadPool(notifierId);

        EmailNotifier.Sender sender = new EmailNotifier.Sender();

        this.pool.submit(sender);

    }

    private class Sender implements Runnable {
        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {

                try {

                    NotifiedAlert alert = notifiedAlerts.take();

                    String alertKey = alert.getLabels().get("alertname") + "|" + alert.getLabels().get("severity");

                    if (Duration.between(lastSentTimestamp.get(alertKey), Instant.now())
                            .compareTo(emailConfig.getSendInterval()) == 1) {
                        EmailPopulatingBuilder email = EmailBuilder
                                .startingBlank()
                                .withHeader("X-Priority", 5)
                                .withSubject(alert.getLabels().get("alertname"))
                                .withPlainText(alert.toString())
                                .from(emailConfig.getUsername());
                        for (String receiver : emailConfig.getReceivers()) {
                            email.to(receiver);
                        }

                        mailer.sendMail(email.buildEmail());
                        lastSentTimestamp.put(alertKey, Instant.now());

                        System.out.println("Consume: " + alert.getLabels().get("alertname")
                                + " queue size: " + notifiedAlerts.size());
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
