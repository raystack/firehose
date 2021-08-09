package io.odpf.firehose.sink.objectstorage.writer.remote;

import io.odpf.firehose.sink.objectstorage.writer.local.FileMeta;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;
import java.util.concurrent.Future;

@AllArgsConstructor
@Data
public class ObjectStorageWriterWorkerFuture {
    private Future future;
    private FileMeta fileMeta;
    private Instant startTime;
}
