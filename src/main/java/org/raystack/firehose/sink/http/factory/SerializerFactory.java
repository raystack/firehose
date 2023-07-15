package org.raystack.firehose.sink.http.factory;

import org.raystack.firehose.config.HttpSinkConfig;
import org.raystack.firehose.config.enums.HttpSinkDataFormatType;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.serializer.JsonWrappedProtoByte;
import org.raystack.firehose.serializer.MessageSerializer;
import org.raystack.firehose.serializer.MessageToJson;
import org.raystack.firehose.serializer.MessageToTemplatizedJson;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.stencil.client.StencilClient;
import org.raystack.stencil.Parser;
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
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            Parser protoParser = stencilClient.getParser(httpSinkConfig.getInputSchemaProtoClass());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return new MessageToJson(protoParser, false, httpSinkConfig.getSinkHttpSimpleDateFormatEnable());
            } else {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return MessageToTemplatizedJson.create(new FirehoseInstrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser);
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getInputSchemaProtoClass() == null || httpSinkConfig.getInputSchemaProtoClass().equals("");
    }
}
