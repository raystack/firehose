package io.odpf.firehose.sink.prometheus.builder;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.odpf.firehose.consumer.TestFeedbackLogMessage;
import io.odpf.firehose.sink.prometheus.PromSinkConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TimeSeriesBuilderUtilsTest {

    @Test
    public void shouldGetMetricListFromMessage() throws InvalidProtocolBufferException {
        Properties metricNameProtoIndexMapping = new Properties();
        metricNameProtoIndexMapping.setProperty("7", "tip_amount");
        metricNameProtoIndexMapping.setProperty("5", "feedback_ratings");
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setCustomerId("customer-id")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        List<PrometheusMetric> metricsFromMessage = TimeSeriesBuilderUtils.getMetricsFromMessage(dynamicMessage, metricNameProtoIndexMapping);
        Assert.assertEquals(2, metricsFromMessage.size());
        metricsFromMessage.sort(Comparator.comparing(PrometheusMetric::getMetricName));
        Assert.assertEquals("feedback_ratings", metricsFromMessage.get(0).getMetricName());
        Assert.assertEquals(5.0, metricsFromMessage.get(0).getMetricValue(), 0.0001);
        Assert.assertEquals("tip_amount", metricsFromMessage.get(1).getMetricName());
        Assert.assertEquals(10000.0, metricsFromMessage.get(1).getMetricValue(), 0.0001);
    }


    @Test
    public void shouldGetLabelsFromMessage() throws InvalidProtocolBufferException {
        Properties labelNameProtoIndexMapping = new Properties();
        labelNameProtoIndexMapping.setProperty("3", "driver_id");
        labelNameProtoIndexMapping.setProperty("4", "customer_id");
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder()
                .setDriverId("DRIVER")
                .setTipAmount(10000)
                .setFeedbackRating(5)
                .setCustomerId("CUSTOMER")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1000000)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        Map<String, String> labelsFromMessage = TimeSeriesBuilderUtils.getLabelsFromMessage(dynamicMessage, labelNameProtoIndexMapping, 1);
        Assert.assertEquals(3, labelsFromMessage.size());
        Assert.assertEquals("DRIVER", labelsFromMessage.get("driver_id"));
        Assert.assertEquals("CUSTOMER", labelsFromMessage.get("customer_id"));
        Assert.assertEquals("1", labelsFromMessage.get(PromSinkConstants.KAFKA_PARTITION));
    }

    @Test
    public void shouldGetNestedMetricListFromMessage() throws InvalidProtocolBufferException {
        Properties metricNameProtoIndexMapping = new Properties();
        Properties m = new Properties();
        m.setProperty("1", "order_completion_time_seconds");
        metricNameProtoIndexMapping.put("15", m);
        long seconds = System.currentTimeMillis() / 1000;
        TestFeedbackLogMessage feedbackLogMessage =
                TestFeedbackLogMessage.newBuilder().
                        setOrderCompletionTime(Timestamp.newBuilder().setSeconds(seconds)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        List<PrometheusMetric> metricsFromMessage = TimeSeriesBuilderUtils.getMetricsFromMessage(dynamicMessage, metricNameProtoIndexMapping);
        Assert.assertEquals(1, metricsFromMessage.size());
        Assert.assertEquals("order_completion_time_seconds", metricsFromMessage.get(0).getMetricName());
        Assert.assertEquals(seconds, metricsFromMessage.get(0).getMetricValue(), 0.0001);
    }

    @Test
    public void shouldGetNestedLabelListFromMessage() throws InvalidProtocolBufferException {
        Properties labelNameProtoIndexMapping = new Properties();
        Properties m = new Properties();
        m.setProperty("1", "order_completion_time_seconds");
        labelNameProtoIndexMapping.put("15", m);
        long seconds = System.currentTimeMillis() / 1000;
        TestFeedbackLogMessage feedbackLogMessage =
                TestFeedbackLogMessage.newBuilder().
                        setOrderCompletionTime(Timestamp.newBuilder().setSeconds(seconds)).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), feedbackLogMessage.toByteArray());
        Map<String, String> labelsFromMessage = TimeSeriesBuilderUtils.getLabelsFromMessage(dynamicMessage, labelNameProtoIndexMapping, 1);
        Assert.assertEquals(2, labelsFromMessage.size());
        Assert.assertEquals(String.valueOf(seconds), labelsFromMessage.get("order_completion_time_seconds"));
        Assert.assertEquals("1", labelsFromMessage.get(PromSinkConstants.KAFKA_PARTITION));
    }
}
