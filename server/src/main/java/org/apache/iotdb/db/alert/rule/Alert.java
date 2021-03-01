package org.apache.iotdb.db.alert.rule;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

public class Alert {

    AlertState state;

    HashMap<String, String> labels;

    HashMap<String, String> annotations;

    // The value at the last evaluation of the alerting expression.
    Object value;

    // The interval during which the condition of this alert held true.
    // ResolvedAt will be 0 to indicate a still active alert.
    Instant activeAt;
    Instant firedAt;
    Instant resolvedAt;
    Instant lastSentAt;
    Instant validUntil;

    public Alert (HashMap<String, String> labels,
                  HashMap<String, String> annotations,
                  Instant activeAt,
                  AlertState state,
                  Object value) {

        this.labels = labels;
        this.annotations = annotations;
        this.activeAt = activeAt;
        this.state = state;
        this.value = value;
    }

    public boolean needsSending() {
        if (state == AlertState.StatePending) {
            return false;
        }

        // if an alert has been resolved since the last send, resend it
        if (resolvedAt != null && resolvedAt.isAfter(lastSentAt)) {
            return true;
        }

        return true;
    }



}
