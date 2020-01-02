package com.gojek.esb.latestSink.http.factory;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.enums.HttpSinkDataFormat;
import com.gojek.esb.serializer.EsbMessageSerializer;
import com.gojek.esb.serializer.EsbMessageToJson;
import com.gojek.esb.serializer.JsonWrappedProtoByte;

import lombok.AllArgsConstructor;

/**
 * SerializerFactory build json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

  private HttpSinkDataFormat httpSinkDataFormat;
  private String protoSchema;
  private StencilClient stencilClient;

  public EsbMessageSerializer build() {
    if (protoSchema == null || protoSchema.equals("")) {
      return new JsonWrappedProtoByte();
    }
    if (httpSinkDataFormat == HttpSinkDataFormat.JSON) {
      ProtoParser protoParser = new ProtoParser(stencilClient, protoSchema);
      return new EsbMessageToJson(protoParser, false);
    } else {
      return new JsonWrappedProtoByte();
    }
  }
}
