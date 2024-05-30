package com.benchmark.util;

import com.codahale.metrics.CsvFileProvider;

import java.io.File;

public class CustomerCsvFileProvider  {

    private String fileName;

    public CustomerCsvFileProvider() {
    }

    public CustomerCsvFileProvider(String fileName) {
        this.fileName = fileName;
    }

    public File getFile(File directory) {
        return new File(directory, fileName+ ".csv");
    }
    public String getFileName() {
        return fileName;
    }
}
