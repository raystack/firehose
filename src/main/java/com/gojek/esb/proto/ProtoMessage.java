package com.gojek.esb.proto;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EglcConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;

import java.lang.reflect.Method;

public class ProtoMessage {
    public static final String CLASS_NAME_NOT_FOUND = "proto class provided in the configuration was not found";
    public static final String INVALID_PROTOCOL_CLASS_MESSAGE = "Invalid proto class provided in the configuration";
    public static final String DESERIALIZE_ERROR_MESSAGE = "Esb message could not be parsed";
    private Method esbMessageParser;

    public ProtoMessage(String protoClassName) {
        this.esbMessageParser = parserMethod(protoClassName);
    }

    public Object get(EsbMessage esbMessage, int protoIndex) throws DeserializerException {
        GeneratedMessageV3 protoMsg = null;
        try {
            protoMsg = (GeneratedMessageV3) esbMessageParser.invoke(null, esbMessage.getLogMessage());
        } catch (ReflectiveOperationException e) {
            throw new DeserializerException(DESERIALIZE_ERROR_MESSAGE, e);
        }
        Descriptors.FieldDescriptor fieldDescriptor = protoMsg.getDescriptorForType().findFieldByNumber(protoIndex);
        return protoMsg.getField(fieldDescriptor);
    }

    private Method parserMethod(String protoClassName) {
        Class<Message> builderClass = null;
        try {
            builderClass = (Class<Message>) Class.forName(protoClassName);
        } catch (ClassNotFoundException e) {
            throw new EglcConfigurationException(CLASS_NAME_NOT_FOUND, e);
        }
        try {
            return builderClass.getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new EglcConfigurationException(INVALID_PROTOCOL_CLASS_MESSAGE, e);
        }
    }
}