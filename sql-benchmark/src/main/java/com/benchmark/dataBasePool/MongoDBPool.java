package com.benchmark.dataBasePool;

import com.benchmark.util.TomlUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MongoDBPool {
    private static MongoClient mongoClient = null;
    private void initClient() {
        Lock lock = new ReentrantLock();
        lock.lock();
        try {
            if(mongoClient==null){
                ConnectionString connectionString = new ConnectionString("mongodb://" + TomlUtil.getMongoDbConfig().get("ip") + ":" + TomlUtil.getMongoDbConfig().get("port"));

                MongoClientSettings.Builder builder = MongoClientSettings.builder();
                builder.applyConnectionString(connectionString);

                CodecRegistry codec = CodecRegistries.fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
                builder.codecRegistry(codec);

                if (TomlUtil.getMongoDbConfig().get("name") != null && TomlUtil.getMongoDbConfig().get("pwd") != null) {
                    String userName = String.valueOf(TomlUtil.getMongoDbConfig().get("name"));
                    String pwd = String.valueOf(TomlUtil.getMongoDbConfig().get("password"));
                    builder.credential(MongoCredential.createScramSha256Credential(userName, "admin", pwd.toCharArray()));

                }
                builder.applyToConnectionPoolSettings(b -> {
                    b.maxSize(500);
                    b.minSize(50);
                    b.maxWaitTime(5, TimeUnit.SECONDS);
                    b.maxConnectionIdleTime(15, TimeUnit.MINUTES);
                    b.maxConnectionLifeTime(2, TimeUnit.HOURS);
                });
                mongoClient=MongoClients.create(builder.build());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            lock.unlock();
        }

    }
    public MongoClient getMongoClient() {
        if(mongoClient==null){
            initClient();
        }
        return mongoClient;
    }
}
