package com.gojek.esb.sink.clevertap;

import com.gojek.esb.config.ClevertapSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.proto.ProtoMessage;
import com.gojek.esb.sink.Sink;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ClevertapSink implements Sink {

    private final String eventName;
    private final String eventType;
    private final int eventTimestampIndex;
    private final int userIdIndex;
    private final Properties fieldMapping;
    private Clevertap clevertap;
    private ProtoMessage protoMessage;
    private ClevertapSinkConfig config;

    public ClevertapSink(ClevertapSinkConfig config, Clevertap clevertap, ProtoMessage protoMessage) {
        this.eventName = config.eventName();
        this.eventType = config.eventType();
        this.eventTimestampIndex = config.eventTimestampIndex();
        this.userIdIndex = config.useridIndex();
        this.fieldMapping = config.getProtoToFieldMapping();
        this.clevertap = clevertap;
        this.protoMessage = protoMessage;
        this.config = config;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessages) throws IOException {

        List<EsbMessage> failedMessages = new ArrayList<>();

        List<ClevertapEvent> events = esbMessages.stream().map(this::toCleverTapEvent).collect(Collectors.toList());
        HttpResponse response = clevertap.sendEvents(events);
        if (callUnsuccessful(response)) {
            failedMessages = esbMessages;
        }
        return failedMessages;
    }

    private ClevertapEvent toCleverTapEvent(EsbMessage esbMessage) {

        return new ClevertapEvent(eventName, eventType, timestamp(esbMessage), userid(esbMessage), eventData(esbMessage));
    }

    private Map<String, Object> eventData(EsbMessage esbMessage) {
        return fieldMapping.keySet().stream().collect(
                Collectors.toMap(fieldIndex -> (String) fieldMapping.get(fieldIndex),
                        fieldIndex -> protoFieldValue(esbMessage, Integer.parseInt(fieldIndex.toString()))));
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

    protected boolean callUnsuccessful(HttpResponse response) {
        return response == null || config.retryStatusCodeRanges().containsKey(status(response));
    }

    protected int status(HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }

    @Override
    public void close() throws IOException {

    }
}
