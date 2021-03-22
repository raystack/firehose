package io.odpf.firehose.sink.prometheus.builder;

import cortexpb.Cortex;
import org.apache.http.entity.ByteArrayEntity;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class RequestEntityBuilder {

    public ByteArrayEntity buildHttpEntity(Cortex.WriteRequest writeRequest) throws IOException {
        byte[] compressed = Snappy.compress(writeRequest.toByteArray());
        return new ByteArrayEntity(compressed);
    }
}
