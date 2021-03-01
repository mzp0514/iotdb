package org.apache.iotdb.db.alert.rule;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;

public class Query {

    static final private Planner processor = new Planner();

    private AggregationPlan aggregationPlan;

    private AggregationExecutor engineExecutor;

    public Query(String timeSeries, String aggregationFunction, String windowPeriod) {

        try {
            this.aggregationPlan = (AggregationPlan) processor.parseSQLToPhysicalPlan(
                    genStatement(timeSeries, aggregationFunction, windowPeriod)
            );
        } catch (QueryProcessException e) {
            e.printStackTrace();
        }
        this.engineExecutor = new AggregationExecutor(aggregationPlan);

    }

    public QueryDataSet query() {

        QueryDataSet dataSet = null;

        try {
            long queryId =
            QueryResourceManager.getInstance()
                    .assignQueryId(true, 1, this.aggregationPlan.getDeduplicatedPaths().size());

            dataSet = engineExecutor.executeWithoutValueFilter(new QueryContext(queryId), this.aggregationPlan);

        } catch (StorageEngineException | IOException | QueryProcessException  e) {
            e.printStackTrace();
        }

        return dataSet;
    }

    private String genStatement(String timeSeries, String aggregationFunction, String windowPeriod) {

        int lastIndex = timeSeries.lastIndexOf(".");
        String path = timeSeries.substring(lastIndex + 1);
        String prefix = timeSeries.substring(0, lastIndex);

        return "select " + aggregationFunction + "(" + path + ")" +
                " from " + prefix + " where time >= now() - " + windowPeriod;
    }



}
