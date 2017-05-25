package com.gojek.esb.consumer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ParallelizerTest {

    private int PARALLELISM = 5;

    @Test
    public void shouldExecuteTask() throws InterruptedException {
        final List<Long> threadList = new ArrayList<>();
        Paralleliser paralleliser = new Paralleliser(PARALLELISM, countDownLatch -> {
            System.out.println(Thread.currentThread().getId());
            threadList.add(Thread.currentThread().getId());
            countDownLatch.countDown();
        });

        paralleliser.run().waitForCompletion();
        Assert.assertEquals(threadList.size(), PARALLELISM);
    }
}

