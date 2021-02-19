package com.gojek.esb.sink.http.factory;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HttpSinkConfig;
import com.gojek.esb.config.enums.HttpSinkDataFormatType;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.serializer.MessageSerializer;
import com.gojek.esb.serializer.MessageToJson;
import com.gojek.esb.serializer.MessageToTemplatizedJson;
import com.gojek.esb.serializer.JsonWrappedProtoByte;
import lombok.AllArgsConstructor;

/**
 * SerializerFactory create json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

    private HttpSinkConfig httpSinkConfig;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;

    public MessageSerializer build() {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            instrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getProtoSchema());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                instrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return new MessageToJson(protoParser, false, true);
            } else {
                instrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return MessageToTemplatizedJson.create(new Instrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser);
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        instrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getProtoSchema() == null || httpSinkConfig.getProtoSchema().equals("");
    }
}
