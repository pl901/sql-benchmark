package com.benchmark.dataBasePool;

import com.benchmark.util.CommonUtil;
import com.benchmark.util.TomlUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

@Slf4j
@Service
public class DaMengPool {


    static HikariDataSource hikariDataSource=null;
    @Autowired
    private CommonUtil commonUtil;
    private void run()  {
        Map<String,Object> dmConfig= TomlUtil.getDMConfig();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:dm://"+dmConfig.get("ip")+":"+dmConfig.get("port"));
        config.setUsername(String.valueOf(dmConfig.get("name")));
        config.setPassword(String.valueOf(dmConfig.get("password")));
        config.setMaximumPoolSize(500);
        config.setPoolName("DaMengPool");
        config.setMetricRegistry(commonUtil.initMetricRegistry("DaMengPool"));
        config.setDriverClassName("dm.jdbc.driver.DmDriver");
        config.setConnectionTimeout(60000);
        hikariDataSource = new HikariDataSource(config);

        try {
            hikariDataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public Connection getConnection() {
        try {
            if(hikariDataSource==null){
                run();
            }
            return hikariDataSource.getConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void shutdown() {
        if(hikariDataSource!=null){
            hikariDataSource.close();
            hikariDataSource=null;
        }
    }



}
