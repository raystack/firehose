package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.EsSinkConfig;
import com.gojek.esb.config.enums.EsSinkMessageType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.serializer.MessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gojek.esb.config.enums.EsSinkRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.EsSinkRequestType.UPDATE_ONLY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class EsRequestHandlerFactoryTest {

    @Mock
    private EsSinkConfig esSinkConfig;

    @Mock
    private Instrumentation instrumentation;

    private MessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnInsertRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(false);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, instrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("ES request mode: {}", INSERT_OR_UPDATE);
        assertEquals(EsUpsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(esSinkConfig.isSinkEsModeUpdateOnlyEnable()).thenReturn(true);
        EsRequestHandlerFactory esRequestHandlerFactory = new EsRequestHandlerFactory(esSinkConfig, instrumentation, "id",
                EsSinkMessageType.JSON, jsonSerializer, "customer_id", "booking", "order_number");
        EsRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        verify(instrumentation, times(1)).logInfo("ES request mode: {}", UPDATE_ONLY);
        assertEquals(EsUpdateRequestHandler.class, requestHandler.getClass());
    }
}
