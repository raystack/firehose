package com.gojek.esb.consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ParallelizerTest {

    private int PARALLELISM = 5;

    @Test
    public void shouldExecuteTaskWithParallelism() throws InterruptedException {
        final List<Long> threadList = new ArrayList<>();
        Paralleliser paralleliser = new Paralleliser(PARALLELISM, 100, countDownLatch -> {
            threadList.add(Thread.currentThread().getId());
            countDownLatch.countDown();
        });

        paralleliser.run().waitForCompletion();
        assertEquals(threadList.size(), PARALLELISM);
    }

    @Test
    public void shouldExecuteTaskUntilStopped() throws InterruptedException {
        final ConcurrentHashMap<Long, String> threadResults = new ConcurrentHashMap<Long, String>();
        Paralleliser paralleliser = new Paralleliser(PARALLELISM, 100, countDownLatch -> {
            try{
                while(!Thread.interrupted()){
                    threadResults.put(Thread.currentThread().getId(), "thread started");
                }
            }finally{
                threadResults.put(Thread.currentThread().getId(), "thread closed");
                countDownLatch.countDown();
            }
        });

        paralleliser.run();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread started");
        }

        paralleliser.stop().waitForCompletion();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread closed");
        }
    }

}

