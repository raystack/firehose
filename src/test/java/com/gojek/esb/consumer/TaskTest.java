package com.gojek.esb.consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TaskTest {

    private int PARALLELISM = 5;

    @Test
    public void shouldExecuteTaskWithParallelism() throws InterruptedException {
        final List<Long> threadList = new ArrayList<>();
        Task task = new Task(PARALLELISM, 100, callback -> {
            threadList.add(Thread.currentThread().getId());
            callback.run();
        });

        task.run().waitForCompletion();
        assertEquals(threadList.size(), PARALLELISM);
    }

    @Test
    public void shouldExecuteTaskUntilStopped() throws InterruptedException {
        final ConcurrentHashMap<Long, String> threadResults = new ConcurrentHashMap<Long, String>();
        Task task = new Task(PARALLELISM, 100, callback -> {
            try{
                while(!Thread.interrupted()){
                    threadResults.put(Thread.currentThread().getId(), "thread started");
                }
            }finally{
                threadResults.put(Thread.currentThread().getId(), "thread closed");
                System.out.println("counting down");
                callback.run();
            }
        });

        task.run();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread started");
        }

        task.stop();
        delayTaskSoWaitCallCatchesUp();
        task.waitForCompletion();

        for (Long key :threadResults.keySet()) {
            assertEquals(threadResults.get(key), "thread closed");
        }
    }

    private void delayTaskSoWaitCallCatchesUp() {
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

