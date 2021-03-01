package org.apache.iotdb.db.alert.notifier;

import java.util.HashMap;

public class NotifiedAlert {

    private HashMap<String, String> labels;

    private HashMap<String, String> annotations;

    public NotifiedAlert() {

    };

    public NotifiedAlert(HashMap<String, String> labels,
                         HashMap<String, String> annotations) {
        this.labels = labels;
        this.annotations = annotations;
    }

    public HashMap<String, String> getLabels() {
        return labels;
    }

    public void setLabels(HashMap<String, String> labels) {
        this.labels = labels;
    }

    public HashMap<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(HashMap<String, String> annotations) {
        this.annotations = annotations;
    }

    @Override
    public String toString() {
        String res = "";
        for (HashMap.Entry<String, String> entry : labels.entrySet()) {
            res += entry.getKey() + ": " + entry.getValue() + "\n";
        }
        res += "\n";
        for (HashMap.Entry<String, String> entry : annotations.entrySet()) {
            res += entry.getKey() + ": " + entry.getValue() + "\n";
        }
        return res;
    }
}
