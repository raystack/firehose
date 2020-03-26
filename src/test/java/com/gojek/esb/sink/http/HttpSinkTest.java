package com.gojek.esb.sink.http;

import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.config.converter.RangeToHashMapConverter;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.NeedToRetry;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.sink.http.request.Request;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private Request request;
    @Mock
    private HttpClient httpClient;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private HttpPut httpPut;
    @Mock
    private HttpPost httpPost;
    @Mock
    private HttpResponse response;
    @Mock
    private StatusLine statusLine;
    @Mock
    private Map<Integer, Boolean> retryStatusCodeRange;

    private List<EsbMessage> esbMessages;

    @Before
    public void setup() {
        initMocks(this);

        esbMessages = new ArrayList<>();

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage = new EsbMessage(null, jsonString.getBytes(), "", 0, 1);

        esbMessages.add(esbMessage);
        esbMessages.add(esbMessage);
    }

    @Test
    public void shouldPrepareRequestDuringPreparationAndCallItDuringExecution() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        when(response.getStatusLine()).thenReturn(statusLine, statusLine);
        when(statusLine.getStatusCode()).thenReturn(200, 200);

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut, httpPost);
        when(request.build(esbMessages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response, response);
        when(httpClient.execute(httpPost)).thenReturn(response, response);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
        httpSink.execute();

        verify(request, times(1)).build(esbMessages);
        verify(httpClient, times(1)).execute(httpPut);
        verify(httpClient, times(1)).execute(httpPost);

    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsGivenRange() throws Exception {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(esbMessages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(response);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient,
                new RangeToHashMapConverter().convert(null, "400-505"));
        httpSink.prepare(esbMessages);
        httpSink.execute();
    }

    @Test(expected = NeedToRetry.class)
    public void shouldThrowNeedToRetryExceptionWhenResponseCodeIsNull() throws Exception {

        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut);

        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(request.build(esbMessages)).thenReturn(httpRequests);
        when(httpClient.execute(httpPut)).thenReturn(null);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
        httpSink.execute();
    }

    @Test(expected = IOException.class)
    public void shouldCatchURISyntaxExceptionAndThrowIOException() throws URISyntaxException, DeserializerException, IOException {
        when(request.build(esbMessages)).thenThrow(new URISyntaxException("", ""));

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.prepare(esbMessages);
    }

    @Test
    public void shouldCatchIOExceptionInAbstractSinkAndCaptureFailedExecutionTelemetry() throws Exception {
        when(httpPut.getURI()).thenReturn(new URI("http://dummy.com"));
        when(httpPost.getURI()).thenReturn(new URI("http://dummy.com"));
        List<HttpEntityEnclosingRequestBase> httpRequests = Arrays.asList(httpPut, httpPost);

        when(request.build(esbMessages)).thenReturn(httpRequests);
        when(httpClient.execute(any(HttpPut.class))).thenThrow(IOException.class);

        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);
        httpSink.pushMessage(esbMessages);

        verify(instrumentation, times(1)).captureFailedExecutionTelemetry(any(IOException.class), anyInt());
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        HttpSink httpSink = new HttpSink(instrumentation, request, httpClient, stencilClient, retryStatusCodeRange);

        httpSink.close();
        verify(stencilClient, times(1)).close();
    }

}
