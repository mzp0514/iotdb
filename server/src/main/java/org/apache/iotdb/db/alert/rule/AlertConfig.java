package org.apache.iotdb.db.alert.rule;

import java.time.Duration;
import java.util.HashMap;

public class AlertConfig {

    String symbol;

    String value;

    Duration holdDuration;

    HashMap<String, String> labels;

    HashMap<String, String> annotations;

    public AlertConfig (String symbol, String value, Duration holdDuration,
                        HashMap<String, String> labels, HashMap<String, String> annotations) {
        this.symbol = symbol;
        this.value = value;
        this.holdDuration = holdDuration;
        this.labels = labels;
        this.annotations = annotations;
    }
}
