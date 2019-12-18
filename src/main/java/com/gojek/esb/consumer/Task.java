package com.gojek.esb.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.gojek.esb.metrics.Instrumentation;

public class Task {

    private final ExecutorService executorService;
    private int parallelism;
    private int threadCleanupDelay;
    private Consumer<Runnable> task;
    private Runnable taskFinishCallback;
    private final CountDownLatch countDownLatch;
    private final List<Future<?>> fnFutures;
    private Instrumentation instrumentation;

    public Task(int parallelism, int threadCleanupDelay, Instrumentation instrumentation, Consumer<Runnable> task) {
        executorService = Executors.newFixedThreadPool(parallelism);
        this.parallelism = parallelism;
        this.threadCleanupDelay = threadCleanupDelay;
        this.task = task;
        this.countDownLatch = new CountDownLatch(parallelism);
        this.fnFutures = new ArrayList<>(parallelism);
        taskFinishCallback = countDownLatch::countDown;
        this.instrumentation = instrumentation;
    }

    public Task run() {
        for (int i = 0; i < parallelism; i++) {
            fnFutures.add(executorService.submit(() -> {
                task.accept(taskFinishCallback);
            }));
        }
        return this;
    }

    public void waitForCompletion() throws InterruptedException {
        instrumentation.logInfo("waiting for completion");
        countDownLatch.await();
    }

    public Task stop() {
        try {
            fnFutures.forEach(consumerThread -> consumerThread.cancel(true));
            Thread.sleep(threadCleanupDelay);
        } catch (InterruptedException e) {
            instrumentation.captureNonFatalError(e, "error stopping tasks");
        }
        return this;
    }
}
