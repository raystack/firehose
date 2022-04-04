package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ClickhouseSink extends AbstractSink {
    private final Instrumentation instrumentation;
    private final ClickHouseRequest request;
    private String queries ="";
    private QueryTemplate queryTemplate;


    public ClickhouseSink(Instrumentation instrumentation,ClickHouseRequest request,QueryTemplate queryTemplate) {
        super(instrumentation, "clickhouse");
        this.instrumentation = instrumentation;
        this.queryTemplate = queryTemplate;
        this.request = request;
    }

    @Override
    protected List<Message> execute() throws Exception {

        CompletableFuture<ClickHouseResponse> future =  request.query(queries).execute();
        try (ClickHouseResponse response = future.get()) {
            instrumentation.logInfo(String.valueOf(response.getSummary().getWrittenRows()));
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        queries = createQueries(messages);
    }

    protected String createQueries(List<Message> messages) {
        return queryTemplate.toQueryStringForMultipleMessages(messages);
    }

    @Override
    public void close() throws IOException {

    }
}
