package org.apache.iotdb.db.alert.notifier;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {

    static NotifierManager notifierManager = new NotifierManager();


    public static void main(String[] args) {
        notifierManager.start();

        ExecutorService executorService = Executors.newCachedThreadPool();
        AlertManagerNotifier notifier = (AlertManagerNotifier) notifierManager.getNotifier("alert_manager_test");
        executorService.execute(new Producer(notifier));

        executorService.shutdown();
    }
}

class Producer implements Runnable {

    AlertManagerNotifier notifier;

    public Producer(AlertManagerNotifier notifier) {
        this.notifier = notifier;
    }

    @Override
    public void run() {
        for (int i = 0; i < 5; i++) {
            try {
                notifier.warning("" + i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("produce: " + i + " queue size: " + notifier.notifiedAlerts.size());
        }

        for (int i = 0; i < 5; i++) {
            try {
                notifier.critical("" + i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("produce: " + i + " queue size: " + notifier.notifiedAlerts.size());
        }


        for (int i = 0; i < 5; i++) {
            try {
                notifier.info("" + i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("produce: " + i + " queue size: " + notifier.notifiedAlerts.size());
        }
    }
}
