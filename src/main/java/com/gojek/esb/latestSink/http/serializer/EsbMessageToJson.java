package com.gojek.esb.latestSink.http.serializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.serializer.EsbMessageJsonSerializer;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.json.simple.JSONObject;

/**
 * EsbMessageToJson Serialize protobuff message content into JSON.
 */
public class EsbMessageToJson implements EsbMessageSerializer {
  private ProtoParser protoParser;
  private Gson gson;
  private boolean preserveFieldNames;

  public EsbMessageToJson(ProtoParser protoParser, boolean preserveFieldNames) {
    this.protoParser = protoParser;
    this.preserveFieldNames = preserveFieldNames;
    this.gson = new GsonBuilder().registerTypeAdapter(EsbMessage.class, new EsbMessageJsonSerializer())
        .setExclusionStrategies(createGsonExclusionStrategy())
        .setFieldNamingStrategy(field -> field.getName().replaceAll("_", "")).create();
    ;
  }

  @Override
  public String serialize(EsbMessage esbMessage) throws DeserializerException {
    try {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("topic", esbMessage.getTopic());

      if (esbMessage.getLogKey() != null && esbMessage.getLogKey().length != 0) {
        DynamicMessage key = protoParser.parse(esbMessage.getLogKey());
        jsonObject.put("logKey", this.gson.toJson(convertDynamicMessageToJson(key, preserveFieldNames)));
      }

      DynamicMessage msg = protoParser.parse(esbMessage.getLogMessage());
      jsonObject.put("logMessage", this.gson.toJson(convertDynamicMessageToJson(msg, preserveFieldNames)));

      return jsonObject.toJSONString();
    } catch (InvalidProtocolBufferException | ParseException e) {
      throw new DeserializerException(e.getMessage());
    }
  }

  private Object convertDynamicMessageToJson(DynamicMessage message, boolean preserveFieldNames)
      throws ParseException, InvalidProtocolBufferException {
    Map<Descriptors.FieldDescriptor, Object> allFields = new HashMap<>();
    List<String> timeStampKeys = new ArrayList<>();

    allFields = message.getAllFields();
    for (Descriptors.FieldDescriptor key : allFields.keySet()) {
      Object field = allFields.get(key);
      boolean fieldIsTimestamp = field instanceof DynamicMessage
          && ((DynamicMessage) field).getDescriptorForType().getName().equals(Timestamp.class.getSimpleName());
      if (fieldIsTimestamp) {
        if (preserveFieldNames) {
          timeStampKeys.add(key.getName());
        } else {
          timeStampKeys.add(key.getJsonName());
        }
      }
    }

    JSONObject tempJsonObject = new JSONObject();
    if (preserveFieldNames) {
      tempJsonObject.put("tempKey", JsonFormat.printer().preservingProtoFieldNames().print(message));
    } else {
      tempJsonObject.put("tempKey", JsonFormat.printer().print(message));
    }

    for (String key : timeStampKeys) {
      convertProtoBuffTimeStampToDateTime(tempJsonObject, "tempKey", key);
    }

    return new JSONParser().parse(tempJsonObject.get("tempKey").toString());
  }

  private JSONObject convertProtoBuffTimeStampToDateTime(JSONObject jsonObject, String parentField,
      String timeStampField) throws ParseException {
    JSONObject parentObject = (JSONObject) new JSONParser().parse(jsonObject.get(parentField).toString());
    String timestampObject = parentObject.get(timeStampField).toString();

    Date date;
    try {
      date = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").parse(timestampObject);
    } catch (java.text.ParseException e) {
      throw new RuntimeException(String.format("Not able to parse date, %s", timestampObject));
    }
    parentObject.put(timeStampField, date);
    jsonObject.put(parentField, gson.toJson(parentObject));

    return jsonObject;
  }

  private ExclusionStrategy createGsonExclusionStrategy() {
    return new ExclusionStrategy() {
      @Override
      public boolean shouldSkipField(FieldAttributes fieldAttributes) {
        return !fieldAttributes.getName().endsWith("_");
      }

      @Override
      public boolean shouldSkipClass(Class<?> aClass) {
        return false;
      }
    };
  }

}
