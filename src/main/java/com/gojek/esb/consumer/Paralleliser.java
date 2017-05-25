package com.gojek.esb.consumer;

import com.gojek.esb.launch.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class Paralleliser {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final ExecutorService executorService;
    private int parallelism;
    private int threadCleanupDelay;
    private Consumer<CountDownLatch> fnToParallelize;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;

    public Paralleliser(int parallelism, int threadCleanupDelay, Consumer<CountDownLatch> fnToParallelize) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.fnToParallelize = fnToParallelize;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
    }

    public Paralleliser run() {
        for(int i=0; i<parallelism; i++){
            fnFutures.add(executorService.submit(() -> {
                fnToParallelize.accept(this.countDownLatch);
            }));
        }
        return this;
    }

    public void waitForCompletion() throws InterruptedException {
       countDownLatch.await();
    }

    public Paralleliser stop() {
        try {
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            Thread.sleep(threadCleanupDelay);
        } catch (Exception e) {
           logger.error("error stopping tasks", e);
        }
        return this;
    }
}
