package com.gojek.esb.sink.http;

import com.gojek.auth.OAuth2Credential;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.config.HTTPSinkConfig;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.AbstractSink;
import com.gojek.esb.sink.SinkFactory;
import com.gojek.esb.sink.http.request.types.Request;
import com.gojek.esb.sink.http.request.RequestFactory;
import com.gojek.esb.sink.http.request.uri.UriParser;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.Map;

/**
 * Factory class to create the HTTP Sink.
 * The esb-log-consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the HTTPSink sink implementation. {@see ParameterizedHTTPSinkConfig}
 */
public class HttpSinkFactory implements SinkFactory {

    @Override
    public AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        HTTPSinkConfig httpSinkConfig = ConfigFactory.create(HTTPSinkConfig.class, configuration);

        CloseableHttpClient closeableHttpClient = newHttpClient(httpSinkConfig);

        Instrumentation instrumentation = new Instrumentation(statsDReporter, HttpSink.class);

        UriParser uriParser = new UriParser(new ProtoParser(stencilClient, httpSinkConfig.getProtoSchema()), httpSinkConfig.getKafkaRecordParserMode());

        Request request = new RequestFactory(httpSinkConfig, stencilClient, uriParser).createRequest();

        return new HttpSink(instrumentation, request, closeableHttpClient, stencilClient, httpSinkConfig.retryStatusCodeRanges(), httpSinkConfig.requestLogStatusCodeRanges());
    }

    private CloseableHttpClient newHttpClient(HTTPSinkConfig httpSinkConfig) {
        Integer maxHttpConnections = httpSinkConfig.getMaxHttpConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectionRequestTimeout(httpSinkConfig.getRequestTimeoutInMs())
                .setConnectTimeout(httpSinkConfig.getRequestTimeoutInMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        if (httpSinkConfig.getHttpSinkOAuth2Enabled()) {
            OAuth2Credential oauth2 = new OAuth2Credential(
                    httpSinkConfig.getHttpSinkOAuth2ClientName(),
                    httpSinkConfig.getHttpSinkOAuth2ClientSecret(),
                    httpSinkConfig.getHttpSinkOAuth2Scope(),
                    httpSinkConfig.getHttpSinkOAuth2AccessTokenURL());
            builder = oauth2.initialize(builder);
        }
        return builder.build();
    }
}
