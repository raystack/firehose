package com.gojek.esb.parser;

import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;


@AllArgsConstructor
public class ProtoParser {
    private StencilClient stencilClient;
    private String protoClassName;

    public DynamicMessage parse(byte[] bytes) throws InvalidProtocolBufferException {
        Descriptors.Descriptor descriptor = stencilClient.get(protoClassName);
        if (descriptor == null) {
            throw new EsbLogConsumerConfigException(String.format("No Descriptors found for %s", protoClassName));
        }
        return DynamicMessage.parseFrom(descriptor, bytes);
    }
}
