package com.gojek.esb.launch;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.FirehoseConsumer;
import com.gojek.esb.consumer.Task;
import com.gojek.esb.factory.FirehoseConsumerFactory;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.metrics.StatsDReporterFactory;
import org.aeonbits.owner.ConfigFactory;

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
        instrumentation.logInfo("Number of consumer threads: " + kafkaConsumerConfig.getApplicationThreadCount());
        instrumentation.logInfo("Delay to clean up consumer threads in ms: " + kafkaConsumerConfig.getApplicationThreadCleanupDelay());

        Task consumerTask = new Task(
                kafkaConsumerConfig.getApplicationThreadCount(),
                kafkaConsumerConfig.getApplicationThreadCleanupDelay(),
                new Instrumentation(statsDReporter, Task.class),
                taskFinished -> {

                    FirehoseConsumer firehoseConsumer = null;
                    try {
                        firehoseConsumer = new FirehoseConsumerFactory(
                                kafkaConsumerConfig,
                                statsDReporter)
                                .buildConsumer();
                        while (true) {
                            if (Thread.interrupted()) {
                                instrumentation.logWarn("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            firehoseConsumer.processPartitions();
                        }
                    } catch (Exception e) {
                        instrumentation.captureFatalError(e, "Exception on creating the consumer, exiting the application");
                        System.exit(1);
                    } finally {
                        ensureThreadInterruptStateIsClearedAndClose(firehoseConsumer, instrumentation);
                        taskFinished.run();
                    }
                });
        instrumentation.logInfo("Consumer Task Created");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            instrumentation.logInfo("Program is going to exit. Have started execution of shutdownHook before this");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();
        instrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FirehoseConsumer firehoseConsumer, Instrumentation instrumentation) {
        Thread.interrupted();
        try {
            firehoseConsumer.close();
        } catch (IOException e) {
            instrumentation.captureFatalError(e, "Exception on closing firehose consumer");
        }
    }
}
