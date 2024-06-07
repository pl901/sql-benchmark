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
public class KingBasePool {


    static HikariDataSource hikariDataSource=null;
    @Autowired
    private CommonUtil commonUtil;
    private void run()  {
        Map<String,Object> kingBaseConfig= TomlUtil.getKingBaseConfig();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:kingbase8://"+kingBaseConfig.get("ip")+":"+kingBaseConfig.get("port")+"/"+kingBaseConfig.get("instance"));
        config.setUsername(String.valueOf(kingBaseConfig.get("name")));
        config.setPassword(String.valueOf(kingBaseConfig.get("password")));
        config.setMaximumPoolSize(500);
        config.setPoolName("kingbasePool");
        config.setMetricRegistry(commonUtil.initMetricRegistry("kingbasePool"));
        config.setDriverClassName("com.kingbase8.Driver");
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
