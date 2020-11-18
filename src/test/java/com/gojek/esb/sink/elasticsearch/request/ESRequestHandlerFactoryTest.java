package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.esb.config.enums.ESRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.ESRequestType.UPDATE_ONLY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class ESRequestHandlerFactoryTest {

    @Mock
    private ESSinkConfig esSinkConfig;

    @Mock
    private Instrumentation instrumentation;

    private EsbMessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnInsertRequestHandler() {
        when(esSinkConfig.isUpdateOnlyMode()).thenReturn(false);
        ESRequestHandlerFactory esRequestHandlerFactory = new ESRequestHandlerFactory(esSinkConfig, instrumentation, "id",
                ESMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        ESRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("ES request mode: {}", INSERT_OR_UPDATE);
        assertEquals(ESUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(esSinkConfig.isUpdateOnlyMode()).thenReturn(true);
        ESRequestHandlerFactory esRequestHandlerFactory = new ESRequestHandlerFactory(esSinkConfig, instrumentation, "id",
                ESMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        ESRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("ES request mode: {}", UPDATE_ONLY);
        assertEquals(ESUpdateRequestHandler.class, requestHandler.getClass());
    }
}
