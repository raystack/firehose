package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClickhouseSink extends AbstractSink {
    private final Instrumentation instrumentation;
    private ClickHouseRequest request;
    private String queries = "";
    private QueryTemplate queryTemplate;
    private List<Message> messageList = new ArrayList<>();
    private ClickHouseClient clickHouseClient;


    public ClickhouseSink(Instrumentation instrumentation, ClickHouseRequest request, QueryTemplate queryTemplate,ClickHouseClient clickHouseClient) {
        super(instrumentation, "clickhouse");
        this.instrumentation = instrumentation;
        this.queryTemplate = queryTemplate;
        this.request = request;
        this.clickHouseClient = clickHouseClient;
    }

    @Override
    protected List<Message> execute() throws Exception {
        request = request.query(queries);
        CompletableFuture<ClickHouseResponse> future =  request.execute();
        try (ClickHouseResponse response = future.get()) {
            ClickHouseResponseSummary clickHouseResponseSummary = response.getSummary();
            instrumentation.logInfo(String.valueOf(clickHouseResponseSummary.getWrittenRows()));
        } catch (ExecutionException | InterruptedException e) {
            messageList.forEach(message -> message.setErrorInfo(new ErrorInfo(e,ErrorType.DEFAULT_ERROR)));
            return messageList;
        }
        return Collections.emptyList();
    }

    @Override
    protected void prepare(List<Message> messages) {
        messageList.clear();
        messageList.addAll(messages);
        queries = queryTemplate.toQueryStringForMultipleMessages(messages);
    }

    @Override
    public void close() throws IOException {
        clickHouseClient.close();
    }
}
