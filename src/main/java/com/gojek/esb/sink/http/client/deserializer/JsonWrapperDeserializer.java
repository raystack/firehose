package com.gojek.esb.sink.http.client.deserializer;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.serializer.EsbMessageJsonSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class JsonWrapperDeserializer implements Deserializer {
    private Gson gson;

    public JsonWrapperDeserializer() {
        this.gson = new GsonBuilder().registerTypeAdapter(EsbMessage.class, new EsbMessageJsonSerializer()).create();
    }

    public List<String> deserialize(List<EsbMessage> messages) {
        return messages.stream().map(this::getParsedJsonMessage).collect(toList());
    }

    private String getParsedJsonMessage(EsbMessage message) {
        return gson.toJson(message);
    }
}
