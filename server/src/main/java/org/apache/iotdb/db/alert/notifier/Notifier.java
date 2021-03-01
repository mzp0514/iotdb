package org.apache.iotdb.db.alert.notifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Notifier {

    private static final Logger logger = LoggerFactory.getLogger(AlertManagerNotifier.class);

    LinkedBlockingQueue<NotifiedAlert> notifiedAlerts = new LinkedBlockingQueue<>();

    ExecutorService pool;

    public void info(String alertname) throws InterruptedException {

        NotifiedAlert alert = new NotifiedAlert();

        HashMap<String, String> labels = new HashMap<>();

        labels.put ("alertname", alertname);
        labels.put ("severity", "info");

        alert.setLabels(labels);
        alert.setAnnotations(new HashMap<>());

        notifiedAlerts.put(alert);

    }

    public void warning(String alertname) throws InterruptedException {
        NotifiedAlert alert = new NotifiedAlert();

        HashMap<String, String> labels = new HashMap<>();

        labels.put ("alertname", alertname);
        labels.put ("severity", "warning");

        alert.setLabels(labels);
        alert.setAnnotations(new HashMap<>());

        notifiedAlerts.put(alert);
    }

    public void critical(String alertname) throws InterruptedException {
        NotifiedAlert alert = new NotifiedAlert();

        HashMap<String, String> labels = new HashMap<>();

        labels.put ("alertname", alertname);
        labels.put ("severity", "critical");

        alert.setLabels(labels);
        alert.setAnnotations(new HashMap<>());

        notifiedAlerts.put(alert);
    }

    public void send(List<NotifiedAlert> newAlerts) {
        synchronized (notifiedAlerts) {
            notifiedAlerts.addAll(newAlerts);
            notifiedAlerts.notifyAll();
        }
    }

    public void stop() {
        if (pool != null) {
            pool.shutdownNow();
            logger.info("Waiting for task pool to shut down");
            waitTermination();
        }
    }

    public void waitAndStop(long milliseconds) {
        if (pool != null) {
            awaitTermination(pool, milliseconds);
            logger.info("Waiting for task pool to shut down");
            waitTermination();
        }
    }

    private void waitTermination() {
        long startTime = System.currentTimeMillis();
        while (!pool.isTerminated()) {
            int timeMillis = 0;
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            timeMillis += 200;
            long time = System.currentTimeMillis() - startTime;
            if (timeMillis % 60_000 == 0) {
                logger.warn("Notifier has wait for {} seconds to stop", time / 1000);
            }
        }
        pool = null;
        logger.info("Notifier stopped");
    }

    private void awaitTermination(ExecutorService service, long milliseconds) {
        try {
            service.shutdown();
            service.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("NotifierThreadPool can not be closed in {} ms", milliseconds);
            Thread.currentThread().interrupt();
        }
        service.shutdownNow();
    }

}
