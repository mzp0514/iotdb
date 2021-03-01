package org.apache.iotdb.db.alert.rule;

import org.apache.iotdb.db.alert.notifier.NotifiedAlert;
import org.apache.iotdb.db.alert.notifier.NotifierManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ThresholdRule extends Rule implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(ThresholdRule.class);

    String name;

    Query query;

    Duration scheduleEvery;

    ConcurrentHashMap<String, Alert> activeAlerts = new ConcurrentHashMap<>();

    ArrayList<AlertConfig> alertConfigs = new ArrayList<>();

    public ThresholdRule (HashMap<String, Object> config) {
        this.name = (String) config.get("name");
        this.query = new Query((String) config.get("time_series"),
                (String) config.get("aggregation_function"),
                (String) config.get("window_period"));
        this.scheduleEvery = (Duration) CommonUtils.parseTimeDuration((String) config.get("schedule_every"));
        List<HashMap<String, Object>> alerts = (List<HashMap<String, Object>>) config.get("alerts");
        for (HashMap<String, Object> map : alerts) {
            alertConfigs.add(new AlertConfig(
                    (String) map.get("symbol"),
                    map.get("value").toString(),
                    (Duration) CommonUtils.parseTimeDuration((String) map.get("for")),
                    (HashMap<String, String>) map.get("labels"),
                    (HashMap<String, String>) map.get("annotations")
            ));
        }
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(scheduleEvery.toMillis());
                schedule(Instant.now());
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean isTriggered(Object value, String threshold, String compSymbol) {
        int compareRes = 0;
        boolean triggered = false;

        if (value instanceof Double) {
            compareRes = ((Double) value).compareTo(Double.parseDouble(threshold));
        }
        else if (value instanceof Float) {
            compareRes = ((Float) value).compareTo(Float.parseFloat(threshold));
        }
        else if (value instanceof Integer) {
            compareRes = ((Integer) value).compareTo(Integer.parseInt(threshold));
        }
        else if (value instanceof Long) {
            compareRes = ((Long) value).compareTo(Long.parseLong(threshold));
        }

        switch (compSymbol) {
            case "<":
                triggered = (compareRes < 0);
                break;

            case "<=":
                triggered = (compareRes <= 0);
                break;

            case ">":
                triggered = (compareRes > 0);
                break;

            case ">=":
                triggered = (compareRes >= 0);
                break;

            case "=":
                triggered = (compareRes == 0);
                break;

            case "!=":
                triggered = (compareRes != 0);
                break;
        }

        return triggered;
    }

    private String genKey(String path, AlertConfig alertConfig) {
        String key = path;

        for(Map.Entry<String, String> entry: alertConfig.labels.entrySet()) {
            key += "&" + entry.getValue();
        }

        return key;
    }

    private void sendAlerts(Instant timestamp) {
        List<NotifiedAlert> notifiedAlerts = new ArrayList<>();
        for (Alert a : activeAlerts.values()) {
            if (a.needsSending()) {
                a.lastSentAt = timestamp;
                notifiedAlerts.add(
                        new NotifiedAlert(a.labels, a.annotations)
                );
            }
        }
        if (!notifiedAlerts.isEmpty()) {
            logger.debug("===================send===================");
            NotifierManager.getInstance().sendAlerts(notifiedAlerts);
        }
    }

    private String fillTemplate(HashMap<String, String> map, String template) {
        if (template == null || map == null)
            return null;
        StringBuffer sb = new StringBuffer();
        Matcher m = Pattern.compile("\\$\\{\\w+\\}").matcher(template);
        while (m.find()) {
            String param = m.group();
            String value = map.get(param.substring(2, param.length() - 1).trim());
            m.appendReplacement(sb, value == null ? "" : value);
        }
        m.appendTail(sb);
        return sb.toString();
    }


    private void schedule(Instant timestamp) {

        logger.debug("========================schedule========================");

        SingleDataSet result = (SingleDataSet) query.query();
        List<Field> fields = result.nextWithoutConstraint().getFields();
        List<Path> paths = result.getPaths();

        for(int i = 0; i < paths.size(); i++) {

            String path = paths.get(i).getFullPath();
            Field field = fields.get(i);

            if (field == null) {
                continue;
            }

            Object value = field.getObjectValue(field.getDataType());

            for (AlertConfig alertConfig : alertConfigs) {

                String threshold = alertConfig.value;
                boolean triggered = isTriggered(value, threshold, alertConfig.symbol);
                String key = genKey(path, alertConfig);
                Alert a = activeAlerts.get(key);

                if (triggered) {
                    HashMap<String, String> lbs = new HashMap<>();
                    lbs.put("alertname", name);
                    lbs.put("timeseries", path);
                    lbs.put("value", value.toString());
                    lbs.putAll(alertConfig.labels);
                    HashMap<String, String> annos = new HashMap<>();

                    for(Map.Entry<String, String> entry : alertConfig.annotations.entrySet()) {
                        annos.put(entry.getKey(), fillTemplate(lbs, entry.getValue()));
                    }

                    if (a == null) {
                        activeAlerts.put(key, new Alert(lbs, annos, Instant.now(),
                                AlertState.StatePending, value));
                    } else {
                        a.value = value;
                        a.annotations = annos;

                        if(a.state == AlertState.StateInactive) {
                            a.state = AlertState.StatePending;
                            a.activeAt = timestamp;
                        }
                        else if (a.state == AlertState.StatePending &&
                                timestamp.minus(alertConfig.holdDuration).isAfter(a.activeAt)) {
                            a.state = AlertState.StateFiring;
                            a.firedAt = timestamp;
                        }
                    }
                }
                else {
                    if (a != null) {
                        if (a.state == AlertState.StatePending || (a.resolvedAt != null)) {
                            activeAlerts.remove(key);
                        }
                        if (a.state != AlertState.StateInactive) {
                            a.state = AlertState.StateInactive;
                            a.resolvedAt = timestamp;
                        }
                    }
                }
            }
        }
        sendAlerts(timestamp);
    }
}
