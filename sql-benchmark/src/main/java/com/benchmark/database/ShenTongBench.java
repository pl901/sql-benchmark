package com.benchmark.database;

import com.benchmark.dataBasePool.ShenTongPool;
import com.benchmark.util.CommonUtil;
import com.benchmark.util.DataUtil;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.benchmark.util.CustomCsvReporter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.benchmark.database.BenchCommon.executor;

@Service
@Slf4j
public class ShenTongBench {
    String tableSql = "CREATE TABLE benchmark.benchmark_table (  id bigint NOT NULL, field1 bigint DEFAULT NULL, field2 bigint DEFAULT NULL,  field3 bigint DEFAULT NULL,  field4 bigint DEFAULT NULL,  field5 varchar(250)  DEFAULT NULL, field6 varchar(250)  DEFAULT NULL,  field7 varchar(1000) DEFAULT NULL,  field8 varchar(1000) DEFAULT NULL,  field9 varchar(1000)   DEFAULT NULL,  PRIMARY KEY (id))";

    //存放统计数据的文件名
    static String fileName = "ShenTongBench";

    @Value("${dataSize}")
    int dataSize;
    @Autowired
    private ShenTongPool shenTongPool;
    @Value("${executeSize}")
    int executeSize;

    @Autowired
    private CommonUtil commonUtil;

    public void startInsert() {
        MetricRegistry metrics = new MetricRegistry();
        CustomCsvReporter reporter = commonUtil.getCustomCsvReporter(metrics, fileName, "insert");
        ConsoleReporter consoleReporter = commonUtil.getConsoleReporter(metrics);


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "神通插入压测开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(ShenTongBench.class, fileName));

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
                String word = "神通插入压测结束---共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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
        try (Connection connection = shenTongPool.getConnection()) {
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


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "神通查询单条记录压测开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(ShenTongBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            queryOne(DataUtil.getRandomNumber(idRange[0], idRange[1]));
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
                String word = "神通查询单条记录压测结束--按照主键查询-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "神通查询压测开始-20一页");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(ShenTongBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            query20(DataUtil.getRandomNumber(idRange[0], idRange[1]));
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
                String word = "神通查询压测结束--每页20-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "神通查询压测开始-50一页");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(ShenTongBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {

                executor.submit(() -> {
                    try {
                        timer.time(() -> {
                            query50(DataUtil.getRandomNumber(idRange[0], idRange[1]));
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
                String word = "神通查询压测结束-每页50--共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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
        try (Connection connection = shenTongPool.getConnection()) {
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
        try (Connection connection = shenTongPool.getConnection()) {
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
        try (Connection connection = shenTongPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public void initData() {
        try (Connection connection = shenTongPool.getConnection()) {
            String sql = "drop SCHEMA  benchmark  ";
            String sql2 = "drop table benchmark.benchmark_table";
            try {
                connection.prepareStatement(sql2).execute();

                connection.prepareStatement(sql).execute();
            } catch (SQLException e) {
            }
            sql = "CREATE SCHEMA  benchmark";
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
        try (Connection connection = shenTongPool.getConnection()) {
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
        try (Connection connection = shenTongPool.getConnection()) {
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
        try (Connection connection = shenTongPool.getConnection()) {
            String sql2 = "drop table benchmark.benchmark_table";

            String sql = "DROP schema benchmark ";
            try {
                connection.prepareStatement(sql2).execute();

                connection.prepareStatement(sql).execute();
            } catch (SQLException e) {

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void updateOne(long id) {
        try (Connection connection = shenTongPool.getConnection()) {
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


        reporter.start(commonUtil.getCsvReportSeconds(), TimeUnit.SECONDS, "神通更新单条数据压测开始");
        consoleReporter.start(commonUtil.getConsoleReportSeconds(), TimeUnit.SECONDS);

        Timer timer = metrics.timer(MetricRegistry.name(ShenTongBench.class, fileName));

        for (int i = 0; i < executeSize; i++) {
            try {
                executor.submit(() -> {

                    timer.time(() -> {
                        try {
                            updateOne(DataUtil.getRandomNumber(idRange[0], idRange[1]));
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
                String word = "神通更新单条数据压测结束--按照主键update-共执行" + executeSize + "次---其中异常" + reporter.getErrorCount() + "次";
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
