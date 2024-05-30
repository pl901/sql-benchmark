package com.benchmark.util;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CommonUtil {
    @Value("${dataSize}")
    int dataSize;
    /**
     * 配置指标监控
     * @param poolName
     * @return
     */
    public MetricRegistry initMetricRegistry(String poolName) {

        MetricRegistry metricRegistry = new MetricRegistry();
        Slf4jReporter reporter = Slf4jReporter.forRegistry(metricRegistry)
                .filter((name, metric) -> name.startsWith(poolName + ".pool"))
                .outputTo( log)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);
        return metricRegistry;
    }
    public String getPath() {
        String path = ".";
        return path;
    }
    public CustomCsvReporter getCustomCsvReporter(MetricRegistry registry,String fileName,String  operationName){
        return CustomCsvReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).
                convertDurationsTo(TimeUnit.MILLISECONDS).withCsvFileProvider(new CustomerCsvFileProvider(fileName+"-"+operationName))
                .build(new File(getPath()));
    }
    public ConsoleReporter getConsoleReporter(MetricRegistry registry){
        return ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).
                convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }
    public int getCsvReportSeconds() {
        return 10;
    }
    public int getConsoleReportSeconds() {
        return 50;
    }
    public Map<String, Object> getDataMap() {
        //分配到每个字段中
        Map<String, Object> map = new HashMap<>();
        map.put("id", SnowflakeIdGenerator.snowflakeIdGenerator.nextId());
        map.put("field1", SnowflakeIdGenerator.snowflakeIdGenerator.nextId());

        map.put("field2", SnowflakeIdGenerator.snowflakeIdGenerator.nextId());

        map.put("field3", SnowflakeIdGenerator.snowflakeIdGenerator.nextId());
        map.put("field4", SnowflakeIdGenerator.snowflakeIdGenerator.nextId());
        String data = DataUtil.getData(dataSize * 1024 - 320);
        map.put("field5", data.substring(0, 249));
        map.put("field6", data.substring(250, 500));
        List<String> stringList = DataUtil.separateString(data.substring(500, data.length()), 3);
        map.put("field7", stringList.get(0));
        map.put("field8", stringList.get(1));
        map.put("field9", stringList.get(2));

        return map;
    }
}
