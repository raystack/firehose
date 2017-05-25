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
    private Consumer<CountDownLatch> fnToParallelize;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;
    private final int DELAY_TO_LET_THREAD_CLEAN_UP = 2000;

    public Paralleliser(int parallelism, Consumer<CountDownLatch> fnToParallelize) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.fnToParallelize = fnToParallelize;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        addShutdownHook();
    }

    public Paralleliser run() {
        for(int i=0; i<parallelism; i++){
            executorService.submit(() ->  fnToParallelize.accept(this.countDownLatch));
        }
        return this;
    }

    public void waitForCompletion() throws InterruptedException {
       countDownLatch.await();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Executing the shutdown hook");

            try {
                fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
                Thread.sleep(DELAY_TO_LET_THREAD_CLEAN_UP);
            } catch (Exception e) {
                e.printStackTrace(); //ignore
            }
        }));
    }
}
