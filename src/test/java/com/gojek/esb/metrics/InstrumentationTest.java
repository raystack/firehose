package com.gojek.esb.metrics;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.util.Clock;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.gojek.esb.metrics.Metrics.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InstrumentationTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private Logger logger;
    @Mock
    private EsbMessage esbMessage;
    @Mock
    private HttpResponse httpResponse;
    @Mock
    private StatusLine statusLine;

    private Instrumentation instrumentation;
    private String testMessage;
    private String testTemplate;
    private Exception e;

    @Before
    public void setUp() {
        instrumentation = new Instrumentation(statsDReporter, logger);
        testMessage = "test";
        testTemplate = "test: {},{},{}";
        e = new Exception();

    }

    @Test
    public void shouldLogString() {
        instrumentation.logInfo(testMessage);
        verify(logger, times(1)).info(testMessage);
    }

    @Test
    public void shouldLogStringTemplate() {

        instrumentation.logInfo(testTemplate, 1, 2, 3);
        verify(logger, times(1)).info(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogDebugStringTemplate() {
        instrumentation.logDebug(testTemplate, 1, 2, 3);
        verify(logger, times(1)).debug(testTemplate, 1, 2, 3);

    }

    @Test
    public void shouldCaptureFilteredMessageCount() {
        String filterExpression = testMessage;
        instrumentation.captureFilteredMessageCount(1, filterExpression);
        verify(statsDReporter, times(1)).captureCount(KAFKA_FILTERED_MESSAGE, 1, "expr=" + filterExpression);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringMessage() {
        instrumentation.captureNonFatalError(e, testMessage);
        verify(logger, times(1)).warn(testMessage);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, NON_FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringTemplate() {
        instrumentation.captureNonFatalError(e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, NON_FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + NON_FATAL_ERROR);

    }

    @Test
    public void shouldCaptureFatalErrorWithStringMessage() {
        instrumentation.captureFatalError(e, testMessage);
        verify(logger, times(1)).error(testMessage);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + FATAL_ERROR);

    }

    @Test
    public void shouldCaptureFatalErrorWithStringTemplate() {
        instrumentation.captureFatalError(e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent(ERROR_EVENT, FATAL_ERROR, ERROR_MESSAGE_TAG + "=" + e.getClass().getName() + ",type=" + FATAL_ERROR);

    }

    @Test
    public void shouldSetStartExecutionTime() {
        Clock clock = new Clock();
        when(statsDReporter.getClock()).thenReturn(clock);
        instrumentation.startExecution();
        Assert.assertEquals(instrumentation.getStartExecutionTime().getEpochSecond(), java.time.Instant.now().getEpochSecond());

    }

    @Test
    public void shouldCaptureSuccessExecutionTelemetry() {
        List<EsbMessage> esbMessages = Collections.singletonList(esbMessage);
        instrumentation.captureSuccessExecutionTelemetry("test", esbMessages);
        verify(logger, times(1)).info("Pushed {} messages to {}.", esbMessages.size(), "test");
        verify(statsDReporter, times(1)).captureDurationSince("response.time", instrumentation.getStartExecutionTime());
        verify(statsDReporter, times(1)).captureCount("messages.count", esbMessages.size(), SUCCESS_TAG);
        verify(statsDReporter, times(esbMessages.size())).captureDurationSince("latency", Instant.ofEpochSecond(esbMessage.getConsumeTimestamp()));
    }

    @Test
    public void shouldCaptureFailedExecutionTelemetry() {
        List<EsbMessage> esbMessages = Collections.singletonList(esbMessage);
        instrumentation.captureFailedExecutionTelemetry(e, esbMessages);
        verify(statsDReporter, times(1)).captureCount("messages.count", esbMessages.size(), FAILURE_TAG);
    }

    @Test
    public void shouldIncrementMessageSucceedCount() {
        instrumentation.incrementMessageSucceedCount();
        verify(statsDReporter, times(1)).increment(RETRY_MESSAGE_COUNT, SUCCESS_TAG);
    }

    @Test
    public void shouldCaptureRetryAttempts() {
        instrumentation.captureRetryAttempts();
        verify(statsDReporter, times(1)).increment(RETRY_ATTEMPTS);
    }

    @Test
    public void shouldIncrementMessageFailCount() {
        instrumentation.incrementMessageFailCount(esbMessage, e);
        verify(statsDReporter, times(1)).increment(RETRY_MESSAGE_COUNT, FAILURE_TAG);
        verify(logger, times(1)).warn(e.getMessage(), e);
    }

    @Test
    public void shouldCaptureLifetimeTillSink() {
        List<EsbMessage> esbMessages = Collections.singletonList(esbMessage);
        instrumentation.lifetimeTillSink(esbMessages);
        verify(statsDReporter, times(esbMessages.size())).captureDurationSince("lifetime.till.sink", Instant.ofEpochSecond(esbMessage.getTimestamp()));
    }

    @Test
    public void shouldCaptureHttpStatusCount() {
        when(httpResponse.getStatusLine()).thenReturn(statusLine);

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(4);
        connectionManager.setDefaultMaxPerRoute(4);

        HttpPut batchPutMethod = new HttpPut("testUrl");
        instrumentation.captureHttpStatusCount(batchPutMethod, httpResponse);

        verify(statsDReporter, times(1)).captureCount(any(), any(), any(), any());
        verify(statusLine, times(1)).getStatusCode();
    }
}
