package io.odpf.firehose.sink.objectstorage.writer;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ScheduledFuture;

@AllArgsConstructor
@Data
public class WriterOrchestratorStatus {
    private boolean isClosed;
    private ScheduledFuture<?> localFileWriterFuture;
    private ScheduledFuture<?> remoteFileWriterFuture;
    private Throwable throwable;
}
