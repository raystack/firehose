package com.gojek.esb.sink.elasticsearch.request;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.config.enums.ESMessageType;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.JsonParseException;
import com.gojek.esb.serializer.EsbMessageToJson;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Base64;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ESInsertRequestHandlerTest {

    @Mock
    private StencilClient stencilClient;

    private EsbMessageToJson jsonSerializer;
    private EsbMessage esbMessageWithJSON;
    private EsbMessage esbMessageWithProto;
    private String logMessage;
    private String jsonString;

    @Before
    public void setUp() {
        initMocks(this);
        jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        esbMessageWithJSON = new EsbMessage(null, jsonString.getBytes(), "", 0, 1);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        esbMessageWithProto = new EsbMessage(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String protoClassName = AggregatedSupplyMessage.class.getName();
        jsonSerializer = new EsbMessageToJson(new ProtoParser(stencilClient, protoClassName), true);

        when(stencilClient.get(protoClassName)).thenReturn(AggregatedSupplyMessage.getDescriptor());
    }

    @Test
    public void shouldReturnTrueForInsertMode() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(
                ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.INSERT_OR_UPDATE, "customer_id");

        assertTrue(esInsertRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnFalseForUpdateOnlyMode() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.UPDATE_ONLY, "customer_id");

        assertFalse(esInsertRequestHandler.canCreate());
    }

    @Test
    public void shouldReturnInsertRequestHandlerForJsonMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.INSERT_OR_UPDATE, "customer_id");

        DocWriteRequest request = esInsertRequestHandler.getRequest(esbMessageWithJSON);
        assertEquals(IndexRequest.class, request.getClass());
    }

    @Test
    public void shouldReturnRequestWithCorrectIdIndexAndTypeForJsonMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.INSERT_OR_UPDATE, "customer_id");

        DocWriteRequest request = esInsertRequestHandler.getRequest(esbMessageWithJSON);
        assertEquals("544131618", request.id());
        assertEquals("booking", request.index());
        assertEquals("customer", request.type());
    }

    @Test
    public void shouldReturnRequestWithCorrectPayloadForJsonMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.INSERT_OR_UPDATE, "customer_id");

        IndexRequest request = (IndexRequest) esInsertRequestHandler.getRequest(esbMessageWithJSON);
        HashMap<String, Object> inputMap = new Gson().fromJson(
                jsonString, new TypeToken<HashMap<String, Object>>() {
                }.getType()
        );
        HashMap<String, Object> outputMap = (HashMap<String, Object>) request.sourceAsMap();

        assertEquals(inputMap.keySet(), outputMap.keySet());
        assertEquals(inputMap.get("wallet_id"), outputMap.get("wallet_id"));
        assertEquals(inputMap.get("dag_run_time"), outputMap.get("dag_run_time"));
    }

    @Test
    public void shouldReturnRequestWithCorrectContentTypeForJsonMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.JSON, jsonSerializer, "customer", "booking", ESRequestType.INSERT_OR_UPDATE, "customer_id");

        IndexRequest request = (IndexRequest) esInsertRequestHandler.getRequest(esbMessageWithJSON);
        assertEquals(XContentType.JSON, request.getContentType());
    }

    @Test
    public void shouldReturnInsertRequestHandlerForProtoMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");

        assertEquals(IndexRequest.class, esInsertRequestHandler.getRequest(esbMessageWithProto).getClass());
    }

    @Test
    public void shouldReturnRequestWithCorrectIdIndexAndTypeForProtoMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");

        DocWriteRequest request = esInsertRequestHandler.getRequest(esbMessageWithProto);
        assertEquals("13", request.id());
        assertEquals("supply", request.index());
        assertEquals("driver", request.type());
    }

    @Test
    public void shouldReturnRequestWithCorrectContentTypeForProtoMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");

        IndexRequest request = (IndexRequest) esInsertRequestHandler.getRequest(esbMessageWithProto);
        assertEquals(XContentType.JSON, request.getContentType());
    }

    @Test
    public void shouldReturnRequestWithCorrectPayloadForProtoMessageType() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");

        IndexRequest request = (IndexRequest) esInsertRequestHandler.getRequest(esbMessageWithProto);
        HashMap<String, Object> outputMap = (HashMap<String, Object>) request.sourceAsMap();

        assertEquals("BIKE", outputMap.get("vehicle_type"));
        assertEquals("3", outputMap.get("unique_drivers"));
    }

    @Test
    public void shouldThrowJSONParseExceptionForInvalidJson() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");

        try {
            esInsertRequestHandler.getFieldFromJSON("", "s2_id_level");
        } catch (Exception e) {
            assertEquals(JsonParseException.class, e.getClass());
        }
    }

    @Test
    public void shouldThrowExceptionForInvalidKey() {
        ESInsertRequestHandler esInsertRequestHandler = new ESInsertRequestHandler(ESMessageType.PROTOBUF, jsonSerializer, "driver", "supply", ESRequestType.INSERT_OR_UPDATE,
                "s2_id_level");
        try {
            esInsertRequestHandler.getFieldFromJSON(jsonSerializer.serialize(esbMessageWithProto), "wrongKey");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Key: wrongKey not found in ESB Message", e.getMessage());
        }
    }
}
