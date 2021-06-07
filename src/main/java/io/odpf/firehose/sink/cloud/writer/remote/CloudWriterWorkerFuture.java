package io.odpf.firehose.sink.cloud.writer.remote;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.Future;

@AllArgsConstructor
@Data
public class CloudWriterWorkerFuture {
    private Future future;
    private String path;
}
