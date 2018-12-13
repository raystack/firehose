package com.gojek.esb.sink.clevertap;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ClevertapEventTest {

    @Test
    public void shouldAssertThatEventsWithSamePropertiesAreEqual() {
        assertEquals(new ClevertapEvent("eventName", "event", 111, "221", null), new ClevertapEvent("eventName", "event", 111, "221", null));
        assertEquals(new ClevertapEvent("eventName", "event", 111, null, null), new ClevertapEvent("eventName", "event", 111, null, null));
    }

    @Test
    public void shouldAssertThatEventsWithDifferentPropertiesAreUnequal() {
        assertNotEquals(new ClevertapEvent("eventName1", "event", 111, "221", null), new ClevertapEvent("eventName", "event", 111, "221", null));
        assertNotEquals(new ClevertapEvent("eventName", "event1", 111, "221", null), new ClevertapEvent("eventName", "event", 111, "221", null));
        assertNotEquals(new ClevertapEvent("eventName", "event", 112, "221", null), new ClevertapEvent("eventName", "event", 111, "221", null));
        assertNotEquals(new ClevertapEvent("eventName", "event", 111, "222", null), new ClevertapEvent("eventName", "event", 111, "221", null));
    }

    @Test
    public void shouldAssertThatEventsWithSameEventDataAreEqual() {
        Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("x", 1);
        eventData1.put("y", 2);
        Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("x", 1);
        eventData2.put("y", 2);
        assertEquals(new ClevertapEvent("eventName", "event", 111, "221", eventData1), new ClevertapEvent("eventName", "event", 111, "221", eventData2));
    }

    @Test
    public void shouldAssertThatEventsWithDifferentEventDataAreUnEqual() {
        Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("x", 1);
        eventData1.put("y", 2);
        Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("x", 1);
        eventData2.put("y", 1);
        assertNotEquals(new ClevertapEvent("eventName", "event", 111, "221", eventData1), new ClevertapEvent("eventName", "event", 111, "221", eventData2));
    }

    @Test
    public void shouldAssertThatEqualEventsHaveTheSameHashCode() {
        Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("x", 1);
        eventData1.put("y", 1);
        Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("x", 1);
        eventData2.put("y", 1);
        assertEquals(new ClevertapEvent("eventName", "event", 111, "221", eventData1).hashCode(),
                     new ClevertapEvent("eventName", "event", 111, "221", eventData2).hashCode());
    }

    @Test
    public void shouldPrintAllFieldsInToString() {
       assertEquals("ClevertapEvent{eventName='eventName', eventType='event', epochTimestamp=111, userId='221', eventData=null}",
                    new ClevertapEvent("eventName", "event", 111, "221", null).toString());
    }
}
