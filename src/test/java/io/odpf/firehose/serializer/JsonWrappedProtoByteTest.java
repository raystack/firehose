package io.odpf.firehose.serializer;

import static org.junit.Assert.assertEquals;

import java.util.Base64;

import io.odpf.firehose.type.Message;
import io.odpf.firehose.exception.DeserializerException;

import org.junit.Before;
import org.junit.Test;

public class JsonWrappedProtoByteTest {

  private Message message;

  @Before
  public void setup() {
    String logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
    String logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
        Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
  }

  @Test
  public void shouldWrapProtoByteInsideJson() throws DeserializerException {
    JsonWrappedProtoByte jsonWrappedProtoByte = new JsonWrappedProtoByte();
    assertEquals("{\"topic\":\"sample-topic\",\"log_key\":\"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC\",\"log_message\":\"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\\u003d\"}", jsonWrappedProtoByte.serialize(message));
  }
}
