package org.raystack.firehose.consumer;

import org.raystack.firehose.message.Message;
import org.raystack.firehose.filter.Filter;
import org.raystack.firehose.filter.FilterException;
import org.raystack.firehose.filter.FilteredMessages;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.metrics.Metrics;
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
