package io.odpf.firehose.consumer;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.filter.Filter;
import io.odpf.firehose.filter.FilterException;
import io.odpf.firehose.filter.FilteredMessages;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.metrics.Metrics;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class FirehoseFilter {
    private final Filter filter;
    private final FirehoseInstrumentation firehoseInstrumentation;

    public FilteredMessages applyFilter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessage = filter.filter(messages);
        int filteredMessageCount = filteredMessage.sizeOfInvalidMessages();
        if (filteredMessageCount > 0) {
            firehoseInstrumentation.captureFilteredMessageCount(filteredMessageCount);
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.FILTERED, filteredMessageCount);
        }
        return filteredMessage;
    }
}
