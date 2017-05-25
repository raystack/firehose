package com.gojek.esb.launch;

import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EsbFilterException;
import com.gojek.esb.factory.LogConsumerFactory;
import org.aeonbits.owner.Config;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, DeserializerException, EsbFilterException, InterruptedException {

        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());
        int parallelism = appConfig.noOfConsumerThreads();
        logger.info("Setting EGLC Parallelism = {}", parallelism);

        final ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
        final CountDownLatch countDownLatch = new CountDownLatch(parallelism);
        final List<Future<?>> kafkaConsumerRunnables = new ArrayList<>(parallelism);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // cancel all the threads, so they get interrupted and they
            logger.info("Executing the shutdown hook");

            try {
                kafkaConsumerRunnables.forEach(consumerThread -> consumerThread.cancel(true));
                Thread.sleep(2000); // give some time to the threads to finish whatever they are doing
            } catch (Exception e) {
                e.printStackTrace(); //ignore
            }
        }));

        for (int i = 0; i < parallelism; ++i) {

            kafkaConsumerRunnables.add(executorService.submit(() -> {

                final LogConsumer logConsumer = new LogConsumerFactory(appConfig, System.getenv()).buildConsumer();

                try {
                    while (true) {
                        try {
                            logger.info("thread interrupted: " + Thread.interrupted());
                            if (!Thread.interrupted())
                                logConsumer.processPartitions();
                            else {
                                logger.info("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                        } catch (IOException | DeserializerException | EsbFilterException e) {
                            logger.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                        }
                    }
                } finally {
                    logConsumer.close();
                    countDownLatch.countDown();
                }
            }));
        }

        try {
            // await for every to finish forever, when the service shutdow, the runHooks will make the threads
            // bump up the countdown latch and this main thread will exit cleanly!
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Main thread interrupted {} {}", e.getMessage(), e);
        }
        logger.info("Exiting main thread");
    }
}
