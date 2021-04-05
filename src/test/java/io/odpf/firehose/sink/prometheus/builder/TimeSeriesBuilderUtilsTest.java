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
        List<Map<String, Object>> metricsFromMessage = TimeSeriesBuilderUtils.getMetricsFromMessage(dynamicMessage, metricNameProtoIndexMapping);
        Assert.assertEquals(2, metricsFromMessage.size());
        metricsFromMessage.sort(Comparator.comparing(o -> o.get(PromSinkConstants.METRIC_NAME).toString()));
        Assert.assertEquals("feedback_ratings", metricsFromMessage.get(0).get(PromSinkConstants.METRIC_NAME));
        Assert.assertEquals(5, metricsFromMessage.get(0).get(PromSinkConstants.METRIC_VALUE));
        Assert.assertEquals("tip_amount", metricsFromMessage.get(1).get(PromSinkConstants.METRIC_NAME));
        Assert.assertEquals((float) 10000.0, metricsFromMessage.get(1).get(PromSinkConstants.METRIC_VALUE));
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
        Map<String, Object> labelsFromMessage = TimeSeriesBuilderUtils.getLabelsFromMessage(dynamicMessage, labelNameProtoIndexMapping, 1);
        Assert.assertEquals(3, labelsFromMessage.size());
        Assert.assertEquals("DRIVER", labelsFromMessage.get("driver_id"));
        Assert.assertEquals("CUSTOMER", labelsFromMessage.get("customer_id"));
        Assert.assertEquals(1, labelsFromMessage.get(PromSinkConstants.KAFKA_PARTITION));
    }
}
