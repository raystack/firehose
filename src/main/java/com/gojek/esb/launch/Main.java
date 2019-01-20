package com.gojek.esb.launch;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.FireHoseConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.factory.FireHoseConsumerFactory;
import com.gojek.esb.filter.EsbFilterException;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        multiThreadedConsumers(kafkaConsumerConfig);
    }

    private static void multiThreadedConsumers(KafkaConsumerConfig kafkaConsumerConfig) throws InterruptedException {
        Task consumerTask = new Task(kafkaConsumerConfig.noOfConsumerThreads(), kafkaConsumerConfig.threadCleanupDelay(), taskFinished -> {

            FireHoseConsumer fireHoseConsumer = null;
            try {
                fireHoseConsumer = new FireHoseConsumerFactory(kafkaConsumerConfig).buildConsumer();
                while (true) {
                    try {
                        if (Thread.interrupted()) {
                            LOGGER.info("Consumer Thread interrupted, leaving the loop!");
                            break;
                        }
                        fireHoseConsumer.processPartitions();
                    } catch (IOException | DeserializerException | EsbFilterException | CommitFailedException e) {
                        LOGGER.error("Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception on creating the consumer, exiting the application", e);
                System.exit(1);
            } finally {
                ensureThreadInterruptStateIsClearedAndClose(fireHoseConsumer);
                taskFinished.run();
            }
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Executing the shutdown hook");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();

        LOGGER.info("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FireHoseConsumer fireHoseConsumer) {
        Thread.interrupted();
        try {
            fireHoseConsumer.close();
        } catch (IOException e) {
            LOGGER.error("Exception on closing firehose consumer", e);
        }
    }
}
