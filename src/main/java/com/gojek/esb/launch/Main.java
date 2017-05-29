package com.gojek.esb.launch;

import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EsbFilterException;
import com.gojek.esb.factory.LogConsumerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, DeserializerException, EsbFilterException, InterruptedException {

        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());
        if(appConfig.noOfConsumerThreads() == 1){
            LogConsumer logConsumer = new LogConsumerFactory(appConfig, System.getenv()).buildConsumer();
            while (true) {
                logConsumer.processPartitions();
            }
        } else {
            multiThreadedConsumers(appConfig);
        }
    }

    private static void multiThreadedConsumers(ApplicationConfiguration appConfig) throws InterruptedException {
        Task consumerTask = new Task(appConfig.noOfConsumerThreads(), appConfig.threadCleanupDelay(), taskFinished -> {

                final LogConsumer logConsumer = new LogConsumerFactory(appConfig, System.getenv()).buildConsumer();

                try {
                    while (true) {
                        try {
                            if (Thread.interrupted()) {
                                logger.info("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            logConsumer.processPartitions();
                        } catch (IOException | DeserializerException | EsbFilterException |CommitFailedException e) {
                            logger.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                        }
                    }
                } catch(Exception e) {
                    logger.error("unhandled exception:", e);
                }
                finally{
                    ensureThreadInterruptStateIsClearedAndClose(logConsumer);
                    taskFinished.run();
                }
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Executing the shutdown hook");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();

        logger.info("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(LogConsumer logConsumer) {
        Thread.interrupted();
        logConsumer.close();
    }
}
