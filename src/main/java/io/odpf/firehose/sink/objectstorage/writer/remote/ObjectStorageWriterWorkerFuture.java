package io.odpf.firehose.sink.objectstorage.writer.remote;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.Future;

@AllArgsConstructor
@Data
public class ObjectStorageWriterWorkerFuture {
    private Future future;
    private String path;
}
