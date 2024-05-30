package com.benchmark.util;



public class SnowflakeIdGenerator {
    public static SnowflakeIdGenerator snowflakeIdGenerator;

    private final long epoch = 1609459200000L; // January 1, 2021 (UTC)

    private final long workerIdBits = 5L;
    private final long dataCenterIdBits = 5L;
    private final long sequenceBits = 12L;

    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

    private final long workerIdShift = sequenceBits;
    private final long dataCenterIdShift = sequenceBits + workerIdBits;
    private final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    private long workerId;
    private long dataCenterId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    static {
        snowflakeIdGenerator =new  SnowflakeIdGenerator();

    }
    public SnowflakeIdGenerator() {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("Worker ID must be between 0 and %d", maxWorkerId));
        }
        if (dataCenterId > maxDataCenterId || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("Datacenter ID must be between 0 and %d", maxDataCenterId));
        }
        this.workerId = 1;
        this.dataCenterId = 1;
    }

    public synchronized long nextId() {
        long timestamp = timeGen();

        if (timestamp < lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards. Refusing to generate ID for %d milliseconds", lastTimestamp - timestamp));
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - epoch) << timestampLeftShift) |
                (dataCenterId << dataCenterIdShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

    // Example usage


    public static void main(String[] args) {
        for(int i=0;i<20;i++){
        System.out.println(    SnowflakeIdGenerator.snowflakeIdGenerator.nextId());

        }
    }


}
