package com.gojek.esb.sink.clevertap;

import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.proto.ProtoMessage;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.http.request.header.BasicHeader;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.newrelic.api.agent.NewRelic;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.gojek.esb.metrics.Metrics.HTTP_RESPONSE_CODE;

public class ClevertapSink extends AbstractSink {
    private String eventName;
    private String eventType;
    private ProtoMessage protoMessage;
    private int userIdIndex;
    private int eventTimestampIndex;
    private Properties fieldMapping;
    private String url;
    private BasicHeader headers;
    private HttpResponse response;
    private HttpClient httpClient;

    private HttpPost request;

    public ClevertapSink(Instrumentation instrumentation, String sinkType, ClevertapSinkConfig config, ProtoMessage protoMessage, HttpClient httpClient) {
        super(instrumentation, sinkType);
        this.eventName = config.eventName();
        this.eventType = config.eventType();
        this.protoMessage = protoMessage;
        this.userIdIndex = config.useridIndex();
        this.eventTimestampIndex = config.eventTimestampIndex();
        this.fieldMapping = config.getProtoToFieldMapping();
        this.url = config.getServiceURL();
        this.headers = new BasicHeader(config.getHTTPHeaders());
        this.httpClient = httpClient;
    }

    @Override
    protected void prepare(List<EsbMessage> esbMessages) {
        List<ClevertapEvent> events = esbMessages.stream().map(this::toCleverTapEvent).collect(Collectors.toList());
        request = new HttpPost(this.url);
        String eventPayload = new GsonBuilder().create().toJson(events);
        getInstrumentation().logDebug("{d:%s}", eventPayload);
        request.setEntity(new StringEntity(String.format("{d:%s}", eventPayload), ContentType.APPLICATION_JSON));
        headers.build().forEach(request::addHeader);
    }

    @Override
    protected List<EsbMessage> execute() throws Exception {
        try {
            response = httpClient.execute(request);
            getInstrumentation().logInfo("Response Status: {}", response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            getInstrumentation().captureFatalError(e, "Error while calling http sink service url");
            NewRelic.noticeError(e);
            throw e;
        } finally {
            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
            captureHttpStatusCount();
        }
        return new ArrayList<>();
    }


    @Override
    public void close() {
    }

    private ClevertapEvent toCleverTapEvent(EsbMessage esbMessage) {
        return new ClevertapEvent(eventName, eventType, timestamp(esbMessage), userid(esbMessage), eventData(esbMessage));
    }

    private Map<String, Object> eventData(EsbMessage esbMessage) {
        return fieldMapping.keySet().stream().collect(
                Collectors.toMap(fieldIndex -> (String) fieldMapping.get(fieldIndex),
                        fieldIndex -> protoFieldValue(esbMessage, Integer.parseInt(fieldIndex.toString()))));
    }

    private void captureHttpStatusCount() {
        String urlTag = "url=" + request.getURI().getPath();
        String httpCodeTag = "status_code=";
        if (response != null) {
            httpCodeTag = "status_code=" + Integer.toString(response.getStatusLine().getStatusCode());
        }
        getInstrumentation().captureCountWithTags(HTTP_RESPONSE_CODE, httpCodeTag, urlTag);
    }

    private Object protoFieldValue(EsbMessage esbMessage, int fieldIndex) {
        try {
            Object fieldValue = protoMessage.get(esbMessage, fieldIndex);
            if (fieldValue instanceof Descriptors.EnumValueDescriptor) {
                return fieldValue.toString();
            } else if (fieldValue instanceof Timestamp) {
                return String.format("$D_%d", ((Timestamp) (fieldValue)).getSeconds());
            } else if (fieldValue instanceof Duration) {
                return ((Duration) (fieldValue)).getSeconds();
            }
            return fieldValue;
        } catch (DeserializerException e) {
            throw new RuntimeException(String.format("Error deserializing field at index %d", fieldIndex), e);
        }
    }

    private String userid(EsbMessage esbMessage) {
        try {
            return (String) protoMessage.get(esbMessage, userIdIndex);
        } catch (DeserializerException e) {
            throw new RuntimeException("Userid field deserialization failed", e);
        }
    }

    private long timestamp(EsbMessage esbMessage) {
        Timestamp eventTimestampField;
        try {
            eventTimestampField = (Timestamp) protoMessage.get(esbMessage, eventTimestampIndex);
        } catch (DeserializerException e) {
            throw new RuntimeException("Eventimestamp field deserialization failed", e);
        }
        return eventTimestampField.getSeconds();
    }
}
