package com.benchmark.database;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    static ExecutorService normal = Executors.newFixedThreadPool(300);
    static ExecutorService visual = Executors.newVirtualThreadPerTaskExecutor();
    static AtomicInteger count=new AtomicInteger(0);
    static  Semaphore semaphore =new Semaphore(10);
    public static void main(String[] args) {
        long start=System.currentTimeMillis();
        for (int i = 0; i <10000 ; i++) {
            normal.submit(()->{
                method();
                count.addAndGet(1);

            });
        }
        while (true){
            if (count.get()==10000){
                long end=System.currentTimeMillis();
                System.out.println("普通线程池300"+(end-start));
                break;
            }
        }
        count=new AtomicInteger(0);
        start=System.currentTimeMillis();
        for (int i = 0; i <10000 ; i++) {
            try {
//                System.out.println(semaphore.availablePermits());
                semaphore.acquire();
                visual.submit(()->{
                    method();
                    count.addAndGet(1);
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                semaphore.release();
            }
        }
        while (true){
            if (count.get()==10000){
                long end=System.currentTimeMillis();
                System.out.println("虚拟线程"+(end-start));
                break;
            }
        }
    }
    public static  synchronized  void method(){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

}
