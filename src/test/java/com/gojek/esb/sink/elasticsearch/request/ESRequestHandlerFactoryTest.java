package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.serializer.EsbMessageToJson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ESRequestHandlerFactoryTest {

    @Mock
    private ESSinkConfig esSinkConfig;

    private EsbMessageToJson jsonSerializer;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldReturnInsertRequestHandler() {
        when(esSinkConfig.isUpdateOnlyMode()).thenReturn(false);
        ESRequestHandlerFactory esRequestHandlerFactory = new ESRequestHandlerFactory(esSinkConfig, "id",
                ESMessageType.JSON, jsonSerializer, "customer_id", "booking");
        ESRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        assertEquals(ESInsertRequestHandler.class, requestHandler.getClass());
    }

    @Test
    public void shouldReturnUpdateRequestHandler() {
        when(esSinkConfig.isUpdateOnlyMode()).thenReturn(true);
        ESRequestHandlerFactory esRequestHandlerFactory = new ESRequestHandlerFactory(esSinkConfig, "id",
                ESMessageType.JSON, jsonSerializer, "customer_id", "booking");
        ESRequestHandler requestHandler = esRequestHandlerFactory.getRequestHandler();

        assertEquals(ESUpdateRequestHandler.class, requestHandler.getClass());
    }
}
