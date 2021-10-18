package io.odpf.firehose.consumer;

import io.odpf.firehose.type.Message;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.filter.FilteredMessages;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class FirehoseFilter {
    private final Filter filter;
    private final Instrumentation instrumentation;

    public FilteredMessages applyFilter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessage = filter.filter(messages);
        int filteredMessageCount = filteredMessage.sizeOfInvalidMessages();
        if (filteredMessageCount > 0) {
            instrumentation.captureFilteredMessageCount(filteredMessageCount);
            instrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.FILTERED, filteredMessageCount);
        }
        return filteredMessage;
    }
}
