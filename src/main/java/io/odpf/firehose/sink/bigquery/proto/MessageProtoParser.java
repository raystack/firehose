package io.odpf.firehose.sink.bigquery.proto;

import com.gojek.de.stencil.client.StencilClient;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MessageProtoParser {
    private final StencilClient stencilClient;
}
