package com.gojek.esb.launch;

import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.consumer.Paralleliser;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EsbFilterException;
import com.gojek.esb.factory.LogConsumerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, DeserializerException, EsbFilterException, InterruptedException {

        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());
        Paralleliser consumerParallerizer = new Paralleliser(appConfig.noOfConsumerThreads(), countDownLatch -> {

                final LogConsumer logConsumer = new LogConsumerFactory(appConfig, System.getenv()).buildConsumer();

                try {
                    while (true) {
                        try {
                            logger.info("thread interrupted: " + Thread.interrupted());
                            if (Thread.interrupted()) {
                                logger.info("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            logConsumer.processPartitions();
                        } catch (IOException | DeserializerException | EsbFilterException e) {
                            logger.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                        }
                    }
                } finally {
                    logConsumer.close();
                    countDownLatch.countDown();
                }
        });

        consumerParallerizer.run().waitForCompletion();

        logger.info("Exiting main thread");
    }
}
