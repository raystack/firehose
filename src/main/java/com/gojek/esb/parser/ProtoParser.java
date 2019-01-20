package com.gojek.esb.parser;

import com.gojek.de.stencil.client.StencilClient;
import com.google.common.base.Strings;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtoParser {
    private Descriptors.Descriptor descriptor;
    private String protoClassName;

    public ProtoParser(StencilClient stencilClient, String protoClassName) {
        this.protoClassName = protoClassName;
        if (!Strings.isNullOrEmpty(protoClassName)) {
            descriptor = stencilClient.get(protoClassName);
        }
    }

    public DynamicMessage parse(byte[] bytes) throws InvalidProtocolBufferException {
        if (descriptor == null) {
            throw new EsbLogConsumerConfigException(String.format("No Descriptors found for %s", protoClassName));
        }
        return DynamicMessage.parseFrom(descriptor, bytes);
    }
}
