package io.odpf.firehose.serializer;

import io.odpf.firehose.consumer.Message;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageJsonSerializer implements JsonSerializer<Message> {

    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject object = new JsonObject();
        object.addProperty("topic", message.getTopic());
        object.addProperty("log_key", message.getSerializedKey());
        object.addProperty("log_message", message.getSerializedMessage());
        return object;
    }
}
