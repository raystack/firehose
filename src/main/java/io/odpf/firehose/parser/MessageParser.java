package io.odpf.firehose.parser;

import io.odpf.firehose.message.Message;

public interface MessageParser {
    Object parse(Message m);
}
