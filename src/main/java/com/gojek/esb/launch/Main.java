package com.gojek.esb.launch;

import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.consumer.LogConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EsbFilterException;
import com.gojek.esb.factory.LogConsumerFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, DeserializerException, EsbFilterException, InterruptedException {

        ApplicationConfiguration appConfig = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());
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
                        } catch (IOException | DeserializerException | EsbFilterException e) {
                            logger.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                        }
                    }
                } catch(InterruptException ignoredInterruptException){

                } catch(Exception e) {
                    logger.error("unhandled exception:", e);
                }
                finally{
                    ensureThreadWasInterruptedAndClose(logConsumer);
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

    private static void ensureThreadWasInterruptedAndClose(LogConsumer logConsumer) {
        Thread.interrupted();
        logConsumer.close();
    }
}
