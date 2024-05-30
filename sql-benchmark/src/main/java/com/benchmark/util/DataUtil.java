package com.benchmark.util;


import java.util.ArrayList;
import java.util.List;

public class DataUtil {
    /**
     * 返回一串固定大小的字符串 单位为B
     * @param size
     * @return
     */
    public static String getData(int size) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < size/10; i++) {
            stringBuilder.append("abcde12345");
        }
        return stringBuilder.toString();
    }
    public static List<String> separateString(String input, int numOfParts) {
        List<String> result = new ArrayList<>();
        int inputLength = input.length();
        int partLength = inputLength / numOfParts;
        int startIndex = 0;
        int endIndex = partLength;
        for (int i = 0; i < numOfParts; i++) {
            if (i == numOfParts - 1) {
                // 最后一部分处理边界情况
                endIndex = inputLength;
            }
            String part = input.substring(startIndex, endIndex);
            result.add(part);
            startIndex = endIndex;
            endIndex += partLength;
        }
        return result;


    }
    public static long  getRandomNumber(long min, long max) {
        return (long) ((Math.random() * (max - min + 1)) + min);
    }
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(getRandomNumber(1,200));

        }
    }
}
