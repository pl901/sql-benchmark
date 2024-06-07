package com.benchmark.util;

import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class TomlUtil {
    static TomlParseResult tomlParseResult;
    static {
        try {
            Path source = Paths.get("./config.toml");
            tomlParseResult = Toml.parse(source);
            tomlParseResult.errors().forEach(error -> System.err.println(error.toString()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static Map<String,Object> getMysqlConfig() {
        TomlTable tomlTable=tomlParseResult.getTable("mysql");
        return tomlTable.toMap();
    }
    public static Map<String,Object> getDMConfig() {
        TomlTable tomlTable=tomlParseResult.getTable("dameng");
        return tomlTable.toMap();
    }
    public static Map<String,Object> getKingBaseConfig() {
        TomlTable tomlTable=tomlParseResult.getTable("kingBase");
        return tomlTable.toMap();
    }
    public static Map<String,Object> getShenTongConfig() {
        TomlTable tomlTable=tomlParseResult.getTable("shenTong");
        return tomlTable.toMap();
    }
    public static int getThreads(){
        return (int) tomlParseResult.getLong("benchmark.threads",()->300);
    }
    public static int getExecuteSize(){
        return (int) tomlParseResult.getLong("benchmark.executeSize",()->500000);
    }
    public static List<String> getExecuteDb(){
        return  tomlParseResult.getArray("benchmark.dbs").toList().stream().map(Object::toString).toList();
    }
    public static int getDataSize(){
        return (int) tomlParseResult.getLong("benchmark.dataSize",()->1);
    }

    public static void main(String[] args) {
        getMysqlConfig();
    }
}
