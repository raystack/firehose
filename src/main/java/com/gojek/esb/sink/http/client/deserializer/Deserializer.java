package com.gojek.esb.sink.http.client.deserializer;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

import java.util.List;

public interface Deserializer {
    /**
     * Wraps binary proto data with JSON.
     * The recipient of the call has to extract and
     * further deserialize the binary proto.
     * @return {@link JsonWrapperDeserializer}
     */
    static Deserializer build() {
        return new JsonWrapperDeserializer();
    }

    /**
     * Deserializes binary proto data into JSON.
     * Falls back to {@link Deserializer#build} if protoClassName
     * is blank.
     *
     * @return {@link JsonDeserializer}
     */
    static Deserializer build(String protoClassName) {
        Deserializer deserializer;
        if (protoClassName == null || protoClassName.equals("")) {
            deserializer = build();
        } else {
            StencilClient stencilClient = StencilClientFactory.getClient();
            ProtoParser protoParser = new ProtoParser(stencilClient, protoClassName);
            deserializer = new JsonDeserializer(protoParser);
        }
        return deserializer;
    }

    List<String> deserialize(List<EsbMessage> messages) throws DeserializerException;
}
