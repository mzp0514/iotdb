package org.apache.iotdb.db.alert.notifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.iotdb.db.alert.notifier.config.AlertManagerNotifierConfig;
import org.apache.iotdb.db.alert.notifier.config.NotifierConfig;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class AlertManagerNotifier extends Notifier {

    static final int BATCH_SIZE = 20;

    AlertManagerNotifierConfig config;

    ObjectMapper objectMapper = new ObjectMapper();

    CloseableHttpClient client = HttpClients.createDefault();


    public AlertManagerNotifier(String notifierId, NotifierConfig config) {
        super();

        this.config = (AlertManagerNotifierConfig) config;

        this.pool = IoTDBThreadPoolFactory.newCachedThreadPool(notifierId);

        Sender sender = new Sender();

        this.pool.submit(sender);

    }


    private class Sender implements Runnable {
        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {

                try {
                    synchronized (notifiedAlerts) {
                        while (notifiedAlerts.isEmpty()) {
                            notifiedAlerts.wait();
                        }
                    }

                    HttpPost request = new HttpPost(config.getHost());

                    List<NotifiedAlert> alertList = new ArrayList<>();

                    while (!notifiedAlerts.isEmpty() && alertList.size() < BATCH_SIZE) {
                        NotifiedAlert alert = notifiedAlerts.take();
                        alertList.add(alert);
                    }

                    if (alertList.isEmpty()) {
                        continue;
                    }

                    String json = objectMapper.writeValueAsString(alertList);

                    System.out.println(json);

                    StringEntity entity = new StringEntity(json);
                    request.setEntity(entity);
                    request.setHeader("Accept", "application/json");
                    request.setHeader("Content-type", "application/json");

                    CloseableHttpResponse response = client.execute(request);
                    System.out.println(response.getStatusLine().getStatusCode());

                } catch (InterruptedException e) {

                    e.printStackTrace();
                    Thread.currentThread().interrupt();

                } catch (IOException e) {

                    e.printStackTrace();

                }

            }
        }

    }

}







