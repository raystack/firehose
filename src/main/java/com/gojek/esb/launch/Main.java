package com.gojek.esb.launch;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.FireHoseConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.factory.FireHoseConsumerFactory;
import com.gojek.esb.filter.EsbFilterException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
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
        StatsDReporter statsDReporter = StatsDReporterFactory
        .fromKafkaConsumerConfig(kafkaConsumerConfig)
        .buildReporter();
        Instrumentation instrumentation = new Instrumentation(statsDReporter, Main.class);
        Task consumerTask = new Task(
            kafkaConsumerConfig.noOfConsumerThreads(),
            kafkaConsumerConfig.threadCleanupDelay(),
            new Instrumentation(statsDReporter, Task.class),
            taskFinished -> {

            FireHoseConsumer fireHoseConsumer = null;
            try {
                fireHoseConsumer = new FireHoseConsumerFactory(kafkaConsumerConfig).buildConsumer();
                while (true) {
                    try {
                        if (Thread.interrupted()) {
                            instrumentation.logInfo("Consumer Thread interrupted, leaving the loop!");
                            break;
                        }
                        fireHoseConsumer.processPartitions();
                    } catch (IOException | DeserializerException | EsbFilterException | CommitFailedException e) {
                        instrumentation.captureNonFatalError(e, "Exception in Consumer Thread {} {} continuing", e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                instrumentation.captureFatalError(e, "Exception on creating the consumer, exiting the application");
                System.exit(1);
            } finally {
                ensureThreadInterruptStateIsClearedAndClose(fireHoseConsumer, instrumentation);
                taskFinished.run();
            }
        });


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            instrumentation.logInfo("Executing the shutdown hook");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();

        instrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FireHoseConsumer fireHoseConsumer, Instrumentation instrumentation) {
        Thread.interrupted();
        try {
            fireHoseConsumer.close();
        } catch (IOException e) {
            instrumentation.captureFatalError(e, "Exception on closing firehose consumer");
        }
    }
}
