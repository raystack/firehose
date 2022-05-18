package io.odpf.firehose.launch;

import io.odpf.depot.config.MetricsConfig;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.metrics.StatsDReporterBuilder;
import io.odpf.firehose.config.KafkaConsumerConfig;
import io.odpf.firehose.consumer.FirehoseConsumer;
import io.odpf.firehose.consumer.FirehoseConsumerFactory;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.metrics.Metrics;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;

/**
 * Main class to run firehose.
 */
public class Main {

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerConfig kafkaConsumerConfig = ConfigFactory.create(KafkaConsumerConfig.class, System.getenv());
        multiThreadedConsumers(kafkaConsumerConfig);
    }

    private static void multiThreadedConsumers(KafkaConsumerConfig kafkaConsumerConfig) throws InterruptedException {
        MetricsConfig config = ConfigFactory.create(MetricsConfig.class, System.getenv());
        StatsDReporter statsDReporter = StatsDReporterBuilder.builder().withMetricConfig(config)
                .withExtraTags(Metrics.tag(Metrics.CONSUMER_GROUP_ID_TAG, kafkaConsumerConfig.getSourceKafkaConsumerGroupId()))
                .build();
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, Main.class);
        firehoseInstrumentation.logInfo("Number of consumer threads: " + kafkaConsumerConfig.getApplicationThreadCount());
        firehoseInstrumentation.logInfo("Delay to clean up consumer threads in ms: " + kafkaConsumerConfig.getApplicationThreadCleanupDelay());

        Task consumerTask = new Task(
                kafkaConsumerConfig.getApplicationThreadCount(),
                kafkaConsumerConfig.getApplicationThreadCleanupDelay(),
                new FirehoseInstrumentation(statsDReporter, Task.class),
                taskFinished -> {

                    FirehoseConsumer firehoseConsumer = null;
                    try {
                        firehoseConsumer = new FirehoseConsumerFactory(kafkaConsumerConfig, statsDReporter).buildConsumer();
                        while (true) {
                            if (Thread.interrupted()) {
                                firehoseInstrumentation.logWarn("Consumer Thread interrupted, leaving the loop!");
                                break;
                            }
                            firehoseConsumer.process();
                        }
                    } catch (Exception | Error e) {
                        firehoseInstrumentation.captureFatalError("firehose_error_event", e, "Caught exception or error, exiting the application");
                        System.exit(1);
                    } finally {
                        ensureThreadInterruptStateIsClearedAndClose(firehoseConsumer, firehoseInstrumentation);
                        taskFinished.run();
                    }
                });
        firehoseInstrumentation.logInfo("Consumer Task Created");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            firehoseInstrumentation.logInfo("Program is going to exit. Have started execution of shutdownHook before this");
            consumerTask.stop();
        }));

        consumerTask.run().waitForCompletion();
        firehoseInstrumentation.logInfo("Exiting main thread");
    }

    private static void ensureThreadInterruptStateIsClearedAndClose(FirehoseConsumer firehoseConsumer, FirehoseInstrumentation firehoseInstrumentation) {
        Thread.interrupted();
        try {
            firehoseConsumer.close();
        } catch (IOException e) {
            firehoseInstrumentation.captureFatalError("firehose_error_event", e, "Exception on closing firehose consumer");
        }
    }
}
