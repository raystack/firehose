package io.odpf.firehose.filter;

import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.Instrumentation;

import java.util.List;

public class NoOpFilter implements Filter {

    public NoOpFilter(Instrumentation instrumentation) {
        instrumentation.logInfo("No filter is selected");
    }

    /**
     * The method used for filtering the messages.
     *
     * @param messages the protobuf records in binary format that are wrapped in {@link Message}
     * @return filtered messages.
     * @throws FilterException the filter exception
     */
    @Override
    public FilteredMessages filter(List<Message> messages) throws FilterException {
        FilteredMessages filteredMessages = new FilteredMessages();
        messages.forEach(filteredMessages::addToValidMessages);
        return filteredMessages;
    }
}
