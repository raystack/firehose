package com.gojek.esb.sink;

import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;

import java.io.IOException;
import java.util.List;

public class SinkDecorator implements Sink {

    private final Sink sink;

    public SinkDecorator(Sink sink) {
        this.sink = sink;
    }

    @Override
    public List<EsbMessage> pushMessage(List<EsbMessage> esbMessage) throws IOException, DeserializerException {
        return this.sink.pushMessage(esbMessage);
    }

    @Override
    public void close() throws IOException {

    }
}
