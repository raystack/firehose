package com.gojek.esb.launch;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.FireHoseConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.factory.FireHoseConsumerFactory;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.StatsDReporterFactory;

import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.CommitFailedException;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        multiThreadedConsumers(kafkaConsumerConfig);
    }

    private static void multiThreadedConsumers(KafkaConsumerConfig kafkaConsumerConfig) throws InterruptedException {
        Instrumentation instrumentation = new Instrumentation(
                StatsDReporterFactory
                .fromKafkaConsumerConfig(kafkaConsumerConfig)
                .buildReporter());
        Task consumerTask = new Task(kafkaConsumerConfig.noOfConsumerThreads(), kafkaConsumerConfig.threadCleanupDelay(), taskFinished -> {

            FireHoseConsumer fireHoseConsumer = null;
            try {
                fireHoseConsumer = new FireHoseConsumerFactory(kafkaConsumerConfig).buildConsumer();
                while (true) {
                    try {
                        if (Thread.interrupted()) {
                            instrumentation.logConsumerThreadInterrupted();
                            break;
                        }
                        fireHoseConsumer.processPartitions();
                    } catch (IOException | DeserializerException | EsbFilterException | CommitFailedException e) {
                        instrumentation.captureConsumerThreadError(e);
                    }
                }
            } catch (Exception e) {
                instrumentation.captureConsumerCreationFailure(e);
                System.exit(1);
            } finally {
                ensureThreadInterruptStateIsClearedAndClose(fireHoseConsumer, instrumentation);
                taskFinished.run();
            }
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            instrumentation.logShutdownHook();
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();

        instrumentation.logExitMainThread();
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FireHoseConsumer fireHoseConsumer, Instrumentation instrumentation) {
        Thread.interrupted();
        try {
            fireHoseConsumer.close();
        } catch (IOException e) {
            instrumentation.captureConsumerCloseError(e);
        }
    }
}
