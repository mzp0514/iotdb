package org.apache.iotdb.db.alert.rule;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class RuleManager implements IService {

    private static final Logger logger = LoggerFactory.getLogger(RuleManager.class);

    private static final RuleManager INSTANCE = new RuleManager();

    private ExecutorService pool;

    private List<Rule> rules = new ArrayList<>();

    public RuleManager() {
        try {
            parseConfig("./server/src/assembly/resources/conf/rule.yml");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static RuleManager getInstance() {
        return INSTANCE;
    }

    private void parseConfig (String configPath) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Reader reader = new FileReader(configPath);
        HashMap<String, Object> obj = yaml.load(reader);

        List<HashMap<String, Object>> ruleConfigs = (List<HashMap<String, Object>>) obj.get("rules");

        if (ruleConfigs != null) {
            for (HashMap<String, Object> map : ruleConfigs) {
                switch ((String) map.get("type")) {
                    case "threshold":
                        ThresholdRule rule = new ThresholdRule(map);
                        rules.add(rule);
                        break;

                    case "deadman":

                        break;
                }
            }
        }

    }

    @Override
    public void start() {
        if (pool == null) {
            this.pool = IoTDBThreadPoolFactory.newCachedThreadPool("Rule Manager");
            for (Rule rule : rules) {
                submitTask(rule);
            }
        }
        logger.info("Rule manager started.");
    }

    @Override
    public void stop() {
        if (pool != null) {
            pool.shutdownNow();
            logger.info("Waiting for task pool to shut down");
            waitTermination();
        }
    }

    @Override
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
                logger.error("RuleManager {} shutdown", e);
                Thread.currentThread().interrupt();
            }
            timeMillis += 200;
            long time = System.currentTimeMillis() - startTime;
            if (timeMillis % 60_000 == 0) {
                logger.warn("RuleManager has wait for {} seconds to stop", time / 1000);
            }
        }
        pool = null;
        logger.info("RuleManager stopped");
    }

    private void awaitTermination(ExecutorService service, long milliseconds) {
        try {
            service.shutdown();
            service.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("RuleThreadPool can not be closed in {} ms", milliseconds);
            Thread.currentThread().interrupt();
        }
        service.shutdownNow();
    }

    @Override
    public ServiceType getID() {
        return ServiceType.RULE_SERVICE;
    }

    public void submitTask(Runnable rule) throws RejectedExecutionException {
        if (pool != null && !pool.isTerminated()) {
            pool.submit(rule);
        }
    }

    public boolean isTerminated() {
        return pool == null || pool.isTerminated();
    }

}
