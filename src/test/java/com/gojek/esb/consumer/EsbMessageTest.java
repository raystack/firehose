package com.gojek.esb.consumer;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class EsbMessageTest {

    private EsbMessage esbMessage;
    private TestMessage message;
    private TestKey key;

    @Before
    public void setUp() {
        message = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();
        key = TestKey.newBuilder().setOrderNumber("123").setOrderUrl("abc").build();

        esbMessage = new EsbMessage(key.toByteArray(), message.toByteArray(), "Topic", 0, 100);
    }

    @Test
    public void encodesKeyToBase64() {
        String actual = esbMessage.getSerializedKey();

        String expected = "CgMxMjMSA2FiYw==";

        assertEquals(expected, actual);
    }

    @Test
    public void nullKeyGetsEncoded() {

        esbMessage = new EsbMessage(null, message.toByteArray(), "Topic", 0, 100);

        assertEquals("", esbMessage.getSerializedKey());
    }

    @Test
    public void emptyKeyGetsEncoded() {

        esbMessage = new EsbMessage(new byte[]{}, message.toByteArray(), "Topic", 0, 100);

        assertEquals("", esbMessage.getSerializedKey());
    }
    @Test
    public void encodesValueToBase64() {
        String actual = esbMessage.getSerializedMessage();

        String expected = "CgMxMjMSA2FiYxoHZGV0YWlscw==";

        assertEquals(expected, actual);
    }
}
