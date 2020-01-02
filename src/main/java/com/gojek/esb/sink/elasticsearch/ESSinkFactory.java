package com.gojek.esb.sink.elasticsearch;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.config.enums.ESRequestType;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.elasticsearch.client.ESSinkClient;
import com.gojek.esb.sink.http.client.deserializer.JsonDeserializer;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.Sink;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

import static com.gojek.esb.config.enums.ESRequestType.INSERT_OR_UPDATE;
import static com.gojek.esb.config.enums.ESRequestType.UPDATE_ONLY;


public class ESSinkFactory implements SinkFactory {

    @Override
    public Sink create(Map<String, String> configuration, StatsDReporter client, StencilClient stencilClient) {
        ESSinkConfig esSinkConfig = ConfigFactory.create(ESSinkConfig.class, configuration);
        ESRequestType esRequestType = esSinkConfig.isUpdateOnlyMode() ? UPDATE_ONLY : INSERT_OR_UPDATE;
        ESRequestBuilder esRequestBuilder = new ESRequestBuilder(esRequestType, esSinkConfig.getEsIdFieldName(), esSinkConfig.shouldPreserveProtoFieldNames(),
                esSinkConfig.getESMessageType(), new JsonDeserializer(new ProtoParser(stencilClient, esSinkConfig.getProtoSchema())));
        ESSinkClient esSinkClient = new ESSinkClient(esSinkConfig, client);
        return new ESSink(esRequestBuilder, esSinkClient, esSinkConfig.getEsTypeName(), esSinkConfig.getEsIndexName());
    }
}
