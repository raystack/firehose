package io.odpf.firehose.filter;

import io.odpf.firehose.metrics.Instrumentation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NoOpFilterTest {

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldLogFilterTypeIfFilterTypeIsNone() {
        new NoOpFilter(instrumentation);
        verify(instrumentation, times(1)).logInfo("No filter is selected");
    }
}
