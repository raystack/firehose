package com.gojek.esb.sink.http.factory;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.serializer.EsbMessageSerializer;
import com.gojek.esb.serializer.EsbMessageToJson;
import com.gojek.esb.serializer.EsbMessageToTemplatizedJson;
import com.gojek.esb.serializer.JsonWrappedProtoByte;
import lombok.AllArgsConstructor;

/**
 * SerializerFactory build json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

    private HTTPSinkConfig httpSinkConfig;
    private StencilClient stencilClient;

    public EsbMessageSerializer build() {
        if (isProtoSchemaEmpty() || httpSinkConfig.getHttpSinkDataFormat() == HttpSinkDataFormat.PROTO) {
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getHttpSinkDataFormat() == HttpSinkDataFormat.JSON) {
            ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getProtoSchema());
            if (httpSinkConfig.getHttpSinkJsonBodyTemplate().isEmpty()) {
                return new EsbMessageToJson(protoParser, false, true);
            } else {
                return new EsbMessageToTemplatizedJson(httpSinkConfig.getHttpSinkJsonBodyTemplate(), protoParser);
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.
        return new JsonWrappedProtoByte();
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getProtoSchema() == null || httpSinkConfig.getProtoSchema().equals("");
    }
}
