package io.odpf.firehose.parser.json;

import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.parser.MessageParser;
import org.json.JSONObject;

public class JsonMessageParser implements MessageParser {
    private AppConfig appConfig;

    public JsonMessageParser(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Override
    public JSONObject parse(Message m) {
        if (appConfig.getKafkaRecordParserMode().equals("key")) {
            return new JSONObject(new String(m.getLogKey()));
        }
        return new JSONObject(new String(m.getLogMessage()));
    }
}
