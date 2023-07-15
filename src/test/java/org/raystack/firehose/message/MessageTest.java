package org.raystack.firehose.message;

import org.raystack.firehose.exception.DefaultException;
import org.raystack.depot.error.ErrorType;
import org.raystack.firehose.consumer.TestKey;
import org.raystack.firehose.consumer.TestMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageTest {

    private Message message;
    private TestMessage testMessage;
    private TestKey key;

    @Before
    public void setUp() {
        testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        message = new Message(key.toByteArray(), testMessage.toByteArray(), "Topic", 0, 100);
    }

    @Test
    public void encodesKeyToBase64() {
        String actual = message.getSerializedKey();

        String expected = "CgMxMjMSA2FiYw==";

        assertEquals(expected, actual);
    }

    @Test
    public void nullKeyGetsEncoded() {

        message = new Message(null, testMessage.toByteArray(), "Topic", 0, 100);

        assertEquals("", message.getSerializedKey());
    }

    @Test
    public void emptyKeyGetsEncoded() {

        message = new Message(new byte[]{}, testMessage.toByteArray(), "Topic", 0, 100);

        assertEquals("", message.getSerializedKey());
    }

    @Test
    public void encodesValueToBase64() {
        String actual = message.getSerializedMessage();

        String expected = "CgMxMjMSA2FiYxoHZGV0YWlscw==";

        assertEquals(expected, actual);
    }

    @Test
    public void shouldSetDefaultError() {
        Assert.assertNull(message.getErrorInfo());
        message.setDefaultErrorIfNotPresent();
        Assert.assertNotNull(message.getErrorInfo());
        Assert.assertEquals(new DefaultException("DEFAULT"), message.getErrorInfo().getException());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, message.getErrorInfo().getErrorType());
    }
}
