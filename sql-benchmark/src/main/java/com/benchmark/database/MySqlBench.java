package com.benchmark.database;

import com.benchmark.util.CommonUtil;
import com.benchmark.util.CustomCsvReporter;
import com.codahale.metrics.*;
import com.benchmark.dataBasePool.MySqlPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.benchmark.util.DataUtil.getRandomNumber;

@Service
@Slf4j
public class MySqlBench {
    String tableSql = "CREATE TABLE benchmark.benchmark_table (\n" + "  id bigint NOT NULL,\n" + "  field1 bigint DEFAULT NULL,\n" + "  field2 bigint DEFAULT NULL,\n" + "  field3 bigint DEFAULT NULL,\n" + "  field4 bigint DEFAULT NULL,\n" + "  field5 varchar(250) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" + "  field6 varchar(250) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" + "  field7 varchar(1000) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" + "  field8 varchar(1000) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" + "  field9 varchar(1000) COLLATE utf8mb4_general_ci DEFAULT NULL,\n" + "  PRIMARY KEY (id)\n" + ")";
    static ExecutorService executor = Executors.newFixedThreadPool(300);

    //存放统计数据的文件名
    static String fileName = "MySqlBench";


    @Autowired
    private MySqlPool mySqlPool;
    @Value("${executeSize}")
    int executeSize;
    @Autowired
    private CommonUtil commonUtil;

    public void startInsert() {
        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "insert");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);

        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mysql插入压测开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(MySqlBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {
                    timer.time(() -> {
                        try {
                            insert();
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
                String word = "mysql插入压测结束---共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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

    public void insert() {
        Map<String, Object> oneData = commonUtil.getDataMap();
        String sql = "insert into benchmark.benchmark_table(id,field1,field2,field3,field4,field5,field6,field7,field8,field9) values(?,?,?,?,?,?,?,?,?,?)";
        try (Connection connection = mySqlPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1, (Long) oneData.get("id"));
            ps.setLong(2, (Long) oneData.get("field1"));
            ps.setLong(3, (Long) oneData.get("field2"));
            ps.setLong(4, (Long) oneData.get("field3"));
            ps.setLong(5, (Long) oneData.get("field4"));
            ps.setString(6, (String) oneData.get("field5"));
            ps.setString(7, (String) oneData.get("field6"));
            ps.setString(8, (String) oneData.get("field7"));
            ps.setString(9, (String) oneData.get("field8"));
            ps.setString(10, (String) oneData.get("field9"));
            ps.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void startQueryOne() {
        Long[] idRange = getRange();
        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "query");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);

        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mysql查询startQueryOne开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(MySqlBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            queryOne(getRandomNumber(idRange[0], idRange[1]));
                        } catch (Exception e) {
                            e.printStackTrace();
                            reporter.reportError();
                        }
                    });


                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        while (true) {
            if (timer.getCount() == executeSize) {
                String word = "mysql查询压测结束--按照主键查询-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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

    public void startQuery20() {

        Long[] idRange = getRange();

        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "query");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);

        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);
        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mysql查询开始");

        Timer timer = metrics.timer(MetricRegistry.name(MySqlBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            query20(getRandomNumber(idRange[0], idRange[1]));
                        } catch (Exception e) {
                            e.printStackTrace();
                            reporter.reportError();
                        }
                    });


                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        while (true) {
            if (timer.getCount() == executeSize) {
                String word = "mysql查询压测结束--每页20-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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

    public void startQuery50() {

        Long[] idRange = getRange();

        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "query");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);

        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);
        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mysql查询开始");

        Timer timer = metrics.timer(MetricRegistry.name(MySqlBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {

                executor.submit(() -> {
                    try {
                        timer.time(() -> {
                            query50(getRandomNumber(idRange[0], idRange[1]));
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                        reporter.reportError();
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        while (true) {
            if (timer.getCount() == executeSize) {
                String word = "mysql查询压测结束-每页50--共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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

    /**
     * @param
     */
    private void query20(long id) {
        String sql = "SELECT id,field1,field2,field3,field4,field5,field6,field7,field8,field9 FROM benchmark.benchmark_table WHERE id >" + id + " LIMIT 0,20";
        try (Connection connection = mySqlPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param
     */
    private void queryOne(long id) {
        String sql = "SELECT id,field1,field2,field3,field4,field5,field6,field7,field8,field9 FROM benchmark.benchmark_table WHERE id =" + id;
        try (Connection connection = mySqlPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     *
     */
    private void query50(long id) {
        String sql = "SELECT id,field1,field2,field3,field4,field5,field6,field7,field8,field9 FROM benchmark.benchmark_table  WHERE id >" + id + " LIMIT 0,50";
        try (Connection connection = mySqlPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public void initData() {
        try (Connection connection = mySqlPool.getConnection()) {
            String sql = "DROP DATABASE benchmark ";
            try {
                connection.prepareStatement(sql).execute();
            } catch (SQLException e) {

            }
            sql = "CREATE DATABASE benchmark CHARACTER SET utf8mb4   COLLATE utf8mb4_general_ci";
            connection.prepareStatement(sql).execute();
            connection.prepareStatement(tableSql).execute();
        } catch (Exception e) {
            System.out.println("初始化数据失败  程序非正常退出");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 计算压测表的数据量 为分页查询提供页数支持
     *
     * @return
     */
    private int count() {
        String sql = "SELECT count(id) FROM benchmark.benchmark_table ";
        try (Connection connection = mySqlPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    private Long[] getRange() {
        Long[] range = new Long[2];
        try (Connection connection = mySqlPool.getConnection()) {
            String sql = "SELECT min(id),max(id) FROM benchmark.benchmark_table ";
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                range[0] = resultSet.getLong(1);
                range[1] = resultSet.getLong(2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return range;
    }

    public void clean() {
        try (Connection connection = mySqlPool.getConnection()) {
            String sql = "DROP DATABASE benchmark ";
            try {
                connection.prepareStatement(sql).execute();
            } catch (SQLException e) {

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void updateOne(long id) {
        try (Connection connection = mySqlPool.getConnection()) {
            String sql = "UPDATE benchmark.benchmark_table SET field1 = 1 WHERE id = " + id;
            connection.prepareStatement(sql).execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void startUpdateOne() {
        Long[] idRange = getRange();
        MetricRegistry metrics = new MetricRegistry();

        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "update");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);

        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);
        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "mysql startUpdateOne开始");

        Timer timer = metrics.timer(MetricRegistry.name(MySqlBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            updateOne(getRandomNumber(idRange[0], idRange[1]));
                        } catch (Exception e) {
                            e.printStackTrace();
                            reporter.reportError();
                        }
                    });


                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        while (true) {
            if (timer.getCount() == executeSize) {
                String word = "mysql update 压测结束--按照主键update-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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

}
