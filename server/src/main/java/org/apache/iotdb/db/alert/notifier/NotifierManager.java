package org.apache.iotdb.db.alert.notifier;

import org.apache.iotdb.db.alert.notifier.config.AlertManagerNotifierConfig;
import org.apache.iotdb.db.alert.notifier.config.EmailNotifierConfig;
import org.apache.iotdb.db.alert.notifier.config.NotifierConfig;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class NotifierManager implements IService {

    private static final Logger logger = LoggerFactory.getLogger(NotifierManager.class);

    private static final NotifierManager INSTANCE = new NotifierManager();

    private ConcurrentHashMap<String, Notifier> notifiers = new ConcurrentHashMap<>();

    public NotifierManager() {
        try {
            parseConfig("./server/src/assembly/resources/conf/alert.yml");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static NotifierManager getInstance() {
        return INSTANCE;
    }

    public void sendAlerts(List<NotifiedAlert> notifiedAlerts) {
        for(Notifier notifier : notifiers.values()) {
            notifier.send(notifiedAlerts);
        }
    }

    public Notifier registerNotifier(String notifierId, NotifierType type, NotifierConfig config) {
        Notifier notifier = null;

        switch (type) {
            case ALERTMANAGER:
                notifier = new AlertManagerNotifier(notifierId, config);
                break;

            case EMAIL:
                notifier = new EmailNotifier(notifierId, config);
                break;
        }

        notifiers.put(notifierId, notifier);

        return notifier;
    }

    public Notifier getNotifier(String notifierId) {
        return notifiers.get(notifierId);
    }

    private void parseConfig(String configPath) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Reader reader = new FileReader(configPath);
        HashMap<String, Object> obj = yaml.load(reader);

        List<HashMap<String, Object>> notifiers = (List<HashMap<String, Object>>) obj.get("notifiers");

        if (notifiers != null) {
            for (HashMap<String, Object> map : notifiers) {
                switch ((String) map.get("type")) {
                    case "alertmanager":
                        AlertManagerNotifierConfig alertmanagerConfig = new AlertManagerNotifierConfig((String) map.get("endpoint"));
                        registerNotifier((String) map.get("name"), NotifierType.ALERTMANAGER, alertmanagerConfig);
                        break;

                    case "email":
                        EmailNotifierConfig emailConfig = new EmailNotifierConfig(
                                (Duration) CommonUtils.parseTimeDuration((String) map.get("send_interval")),
                                "SMTPS",
                                (String) map.get("smtp_host"),
                                (Integer) map.get("smtp_port"),
                                (String) map.get("smtp_auth_username"),
                                (String) map.get("smtp_auth_username"),
                                (List<String>) map.get("receivers")
                        );
                        registerNotifier((String) map.get("name"), NotifierType.EMAIL, emailConfig);
                        break;
                }
            }
        }
    }

    @Override
    public void start() {
        logger.info("Notifier manager started.");
    }

    @Override
    public void stop() {
        for (Notifier notifier : notifiers.values()) {
            notifier.stop();
        }
    }

    @Override
    public void waitAndStop(long milliseconds) {
        for (Notifier notifier : notifiers.values()) {
            notifier.waitAndStop(milliseconds);
        }
    }

    @Override
    public ServiceType getID() {
        return ServiceType.NOTIFIER_SERVICE;
    }

}
