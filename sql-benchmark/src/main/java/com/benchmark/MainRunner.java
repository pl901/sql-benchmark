package com.benchmark;

import com.benchmark.dataBasePool.MySqlPool;
import com.benchmark.database.DaMengBench;
import com.benchmark.database.KingBaseBench;
import com.benchmark.database.MySqlBench;
import com.benchmark.database.ShenTongBench;
import com.benchmark.util.TomlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
@Order(100)
public class MainRunner implements ApplicationRunner {
    List<String> dbs = TomlUtil.getExecuteDb();
    @Autowired
    MySqlBench mySqlBench;
    @Autowired
    DaMengBench daMengBench;
    @Autowired
    KingBaseBench kingBaseBench;
    @Autowired
    ShenTongBench shenTongBench;
    @Autowired
    private MySqlPool mySqlPool;
    ExecutorService ed = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try {
            for (String db : dbs) {
                if (StringUtils.equals(db, "mysql")) {
                    System.out.println("初始化库表");
                    mySqlBench.initData();
                    System.out.println("预热");
                    mySqlBench.insert();
                    System.out.println("预热结束");

                    System.out.println("插入压测开始");
                    mySqlBench.startInsert();
                    //                mySqlPool.shutdown();
                    System.out.println("插入压测结束");

                    System.out.println("更新压测开始");
                    mySqlBench.startUpdateOne();
                    System.out.println("更新压测结束");

                    System.out.println("查询压测开始 只查一条");
                    mySqlBench.startQueryOne();
                    System.out.println("查询压测结束 只查一条");

                    System.out.println("查询压测开始 20一页");
                    mySqlBench.startQuery20();
                    //                mySqlPool.shutdown();
                    System.out.println("查询压测结束 20一页");

                    System.out.println("查询压测开始 50一页");
                    mySqlBench.startQuery50();
                    System.out.println("查询压测结束 50一页");

                    System.out.println("清理数据");
                    mySqlBench.clean();
                    System.out.println("清理数据结束");

                }
                if (StringUtils.equals(db, "dm")) {
                    System.out.println("初始化库表");
                    daMengBench.initData();
                    System.out.println("预热");
                    daMengBench.insert();
                    System.out.println("预热结束");

                    System.out.println("插入压测开始");
                    daMengBench.startInsert();
                    //                mySqlPool.shutdown();
                    System.out.println("插入压测结束");

                    System.out.println("更新压测开始");
                    daMengBench.startUpdateOne();
                    System.out.println("更新压测结束");

                    System.out.println("查询压测开始 只查一条");
                    daMengBench.startQueryOne();
                    System.out.println("查询压测结束 只查一条");

                    System.out.println("查询压测开始 20一页");
                    daMengBench.startQuery20();
                    //                mySqlPool.shutdown();
                    System.out.println("查询压测结束 20一页");

                    System.out.println("查询压测开始 50一页");
                    daMengBench.startQuery50();
                    System.out.println("查询压测结束 50一页");

                    System.out.println("清理数据");
                    daMengBench.clean();
                    System.out.println("清理数据结束");

                }
                if (StringUtils.equals(db, "kingbase")) {
                    System.out.println("初始化库表");
                    kingBaseBench.initData();
                    System.out.println("预热");
                    kingBaseBench.insert();
                    System.out.println("预热结束");

                    System.out.println("插入压测开始");
                    kingBaseBench.startInsert();
                    //                mySqlPool.shutdown();
                    System.out.println("插入压测结束");

                    System.out.println("更新压测开始");
                    kingBaseBench.startUpdateOne();
                    System.out.println("更新压测结束");

                    System.out.println("查询压测开始 只查一条");
                    kingBaseBench.startQueryOne();
                    System.out.println("查询压测结束 只查一条");

                    System.out.println("查询压测开始 20一页");
                    kingBaseBench.startQuery20();
                    //                mySqlPool.shutdown();
                    System.out.println("查询压测结束 20一页");

                    System.out.println("查询压测开始 50一页");
                    kingBaseBench.startQuery50();
                    System.out.println("查询压测结束 50一页");

                    System.out.println("清理数据");
                    kingBaseBench.clean();
                    System.out.println("清理数据结束");

                }
                if (StringUtils.equals(db, "shentong")) {
                    System.out.println("初始化库表");
                    shenTongBench.initData();
                    System.out.println("预热");
                    shenTongBench.insert();
                    System.out.println("预热结束");

                    System.out.println("插入压测开始");
                    shenTongBench.startInsert();
                    //                mySqlPool.shutdown();
                    System.out.println("插入压测结束");

                    System.out.println("更新压测开始");
                    shenTongBench.startUpdateOne();
                    System.out.println("更新压测结束");

                    System.out.println("查询压测开始 只查一条");
                    shenTongBench.startQueryOne();
                    System.out.println("查询压测结束 只查一条");

                    System.out.println("查询压测开始 20一页");
                    shenTongBench.startQuery20();
                    //                mySqlPool.shutdown();
                    System.out.println("查询压测结束 20一页");

                    System.out.println("查询压测开始 50一页");
                    shenTongBench.startQuery50();
                    System.out.println("查询压测结束 50一页");

                    System.out.println("清理数据");
                    shenTongBench.clean();
                    System.out.println("清理数据结束");

                }
                if (StringUtils.equals(db, "mongodb")) {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("发生异常退出");
            System.exit(1);

        }
        System.exit(0);
    }
}
