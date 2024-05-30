package com.benchmark.util;

import com.codahale.metrics.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public class CustomCsvReporter extends ScheduledReporter {
    private static final String DEFAULT_SEPARATOR = ",";
    private AtomicInteger errorCount = new AtomicInteger();

    private String eventName = "";
    private static String lastHeader = "速率,最小时间,最大时间,平均时间,75%,95%,99%,99.9%";
    private static String lastData = "";

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }


    public static class Builder {
        private final MetricRegistry registry;
        private Locale locale;
        private String separator;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private CustomerCsvFileProvider csvFileProvider;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.separator = DEFAULT_SEPARATOR;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
            this.csvFileProvider = new CustomerCsvFileProvider();
        }


        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }


        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }


        public Builder formatFor(Locale locale) {
            this.locale = locale;
            return this;
        }


        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }


        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }


        public Builder withSeparator(String separator) {
            this.separator = separator;
            return this;
        }


        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }


        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder withCsvFileProvider(CustomerCsvFileProvider csvFileProvider) {
            this.csvFileProvider = csvFileProvider;
            return this;
        }


        public CustomCsvReporter build(File directory) {
            return new CustomCsvReporter(registry,
                    directory,
                    locale,
                    separator,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    csvFileProvider);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomCsvReporter.class);

    private final File directory;
    private final Locale locale;
    private final String separator;
    private final Clock clock;
    private final CustomerCsvFileProvider csvFileProvider;

    private final String histogramFormat;
    private final String meterFormat;
    private final String timerFormat;

    private final String timerHeader;
    private final String meterHeader;
    private final String histogramHeader;

    private CustomCsvReporter(MetricRegistry registry,
                              File directory,
                              Locale locale,
                              String separator,
                              TimeUnit rateUnit,
                              TimeUnit durationUnit,
                              Clock clock,
                              MetricFilter filter,
                              ScheduledExecutorService executor,
                              boolean shutdownExecutorOnStop,
                              CustomerCsvFileProvider csvFileProvider) {
        super(registry, "csv-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
        this.directory = directory;
        this.locale = locale;
        this.separator = separator;
        this.clock = clock;
        this.csvFileProvider = csvFileProvider;

        this.histogramFormat = String.join(separator, "%d", "%d", "%f", "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f");
        this.meterFormat = String.join(separator, "%d", "%f", "%f", "%f", "%f", "events/%s");
        this.timerFormat = String.join(separator, "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "calls/%s", "%s");

        this.timerHeader = String.join(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999", "速率", "m1_rate", "m5_rate", "m15_rate", "rate_unit", "duration_unit");
        this.meterHeader = String.join(separator, "count", "速率", "m1_rate", "m5_rate", "m15_rate", "rate_unit");
        this.histogramHeader = String.join(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999");
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp,
                name,
                timerHeader,
                timerFormat,
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        report(timestamp,
                name,
                meterHeader,
                meterFormat,
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        report(timestamp,
                name,
                histogramHeader,
                histogramFormat,
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile());
    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        report(timestamp, name, "count", "%d", counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge<?> gauge) {
        report(timestamp, name, "value", "%s", gauge.getValue());
    }

    private void report(long timestamp, String name, String header, String line, Object... values) {
        try {
            final File file = csvFileProvider.getFile(directory);
            final boolean fileAlreadyExists = file.exists();
            if (fileAlreadyExists || file.createNewFile()) {
                try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        new FileOutputStream(file, true), UTF_8))) {
                    if (!fileAlreadyExists) {
                        out.println(this.eventName);
                        out.println("时间" + separator + header);
                    }
                    Instant instant = Instant.ofEpochSecond(timestamp);
                    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                    out.printf(locale, String.format(locale, "%s" + separator + "%s%n", dateTime.toString(), line), values);
                    lastData = String.format("%.3f",values[11]) + "," +  String.format("%.3f",values[3]) + ","
                            +  String.format("%.3f",values[1]) + "," +  String.format("%.3f",values[2]) + "," +String.format("%.3f",values[6])
                            +  "," +  String.format("%.3f",values[7]) + "," + String.format("%.3f", values[9]) + "," +  String.format("%.3f",values[10]);

                }
            }
        } catch (IOException e) {
            LOGGER.warn("Error writing to {}", name, e);
        }
    }

    public void reportError() {
        errorCount.addAndGet(1);
    }

    public int getErrorCount() {

        return errorCount.get();
    }

    public void start(long period, TimeUnit unit, String eventName) {
        super.start(period, unit);
        this.eventName = eventName;
        errorCount.set(0);
    }


    public void close(String word) {
        super.close();

        //打印一下结尾
        try {
            final File file = csvFileProvider.getFile(directory);
            final boolean fileAlreadyExists = file.exists();
            if (fileAlreadyExists || file.createNewFile()) {
                try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        new FileOutputStream(file, true), UTF_8))) {

                    out.printf(locale, word);
                    out.println();
                    out.println();
                    out.println();

                }
            }
        } catch (IOException e) {
            LOGGER.warn("Error writing to {}", e);
        }
        errorCount.set(0);
        // 输出到合并的文件中
        final File file = new File(directory, "SqlBench-summary.csv");
        final boolean fileAlreadyExists = file.exists();
        try {
            if (fileAlreadyExists || file.createNewFile()) {
                try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(file, true))) {
                    fileWriter.newLine();

                    fileWriter.write(word);
                    fileWriter.newLine();
                    fileWriter.write(lastHeader);
                    fileWriter.newLine();
                    fileWriter.write(lastData);
                    fileWriter.newLine();

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

