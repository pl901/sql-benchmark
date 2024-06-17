package com.benchmark.database;

import com.benchmark.dataBasePool.MongoDBPool;
import com.benchmark.util.CommonUtil;
import com.benchmark.util.CustomCsvReporter;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mongodb.client.MongoCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.benchmark.database.BenchCommon.executeSize;
import static com.benchmark.database.BenchCommon.executor;

@Service
public class MongoDBBench {
    @Autowired

    private CommonUtil commonUtil;
    @Autowired
    private MongoDBPool mongoDBPool;
    static String fileName = "MongoDBBench";
    private MongoCollection mongoCollection= mongoDBPool.getMongoClient().getDatabase("benchmark").getCollection("benchmark_collection");
    public void initData() {
    }
    public void startInsert() {
        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "insert");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mongodb插入压测开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(MongoDBBench.class, fileName));
        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {
                    timer.time(() -> {
                        try {
                            insertOne();
                        } catch (Exception e) {
                            e.printStackTrace();
                            reporter.reportError();
                        }
                    });
                });


            } catch (Exception e) {
                e.printStackTrace();
                reporter.reportError();
            }
        }
        while (true) {
            if (timer.getCount() == executeSize) {
                String word = "mongodb插入压测结束---共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
                reporter.close(word);
                consoleReporter.close();
                System.out.println(word);
                break;
            }
        }
        try {
            //歇一下
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    private  void insertOne() {
        Map<String, Object>  map=commonUtil.getDataMap();
        mongoCollection.insertOne(map);
    }
}
