package io.odpf.firehose.message;

import io.odpf.depot.common.Tuple;
import io.odpf.depot.message.OdpfMessage;

import java.util.List;
import java.util.stream.Collectors;

public class FirehoseMessageUtils {

    public static List<OdpfMessage> convertToOdpfMessage(List<Message> messages) {
        return messages.stream().map(message ->
                        new OdpfMessage(
                                message.getLogKey(),
                                message.getLogMessage(),
                                new Tuple<>("message_topic", message.getTopic()),
                                new Tuple<>("message_partition", message.getPartition()),
                                new Tuple<>("message_offset", message.getOffset()),
                                new Tuple<>("message_headers", message.getHeaders()),
                                new Tuple<>("message_timestamp", message.getTimestamp()),
                                new Tuple<>("load_time", message.getConsumeTimestamp())))
                .collect(Collectors.toList());
    }
}
