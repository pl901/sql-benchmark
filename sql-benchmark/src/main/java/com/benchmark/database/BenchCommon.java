package com.benchmark.database;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Order(1)
public class BenchCommon implements ApplicationRunner {
    @Value("${threads:300}")
    int threads;

    static ExecutorService executor=null;



    private ExecutorService initExecutor() {
        Lock lock=new ReentrantLock();
        try {
            lock.lock();
            if(executor==null){
                executor=Executors.newFixedThreadPool(threads);

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        return executor;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        initExecutor();
    }
}
