package com.gojek.esb.latestSink.clevertap;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class ClevertapEvent {

    @SerializedName("evtName")
    private String eventName;
    @SerializedName("type")
    private String eventType;
    @SerializedName("ts")
    private long epochTimestamp;
    @SerializedName("identity")
    private String userId;
    @SerializedName("evtData")
    private Map<String, Object> eventData;

    public ClevertapEvent(String eventName, String eventType, long epochTimestamp, String userId, Map<String, Object> eventData) {
        this.eventName = eventName;
        this.eventType = eventType;
        this.epochTimestamp = epochTimestamp;
        this.userId = userId;
        this.eventData = eventData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClevertapEvent that = (ClevertapEvent) o;

        if (epochTimestamp != that.epochTimestamp) {
            return false;
        }
        if (!eventName.equals(that.eventName)) {
            return false;
        }
        if (!eventType.equals(that.eventType)) {
            return false;
        }
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) {
            return false;
        }
        return eventData != null ? eventData.equals(that.eventData) : that.eventData == null;

    }

    @Override
    public int hashCode() {
        int result = eventName.hashCode();
        final int hashMultiplier = 31;
        final int hashBitShift = 32;
        result = hashMultiplier * result + eventType.hashCode();
        result = hashMultiplier * result + (int) (epochTimestamp ^ (epochTimestamp >>> hashBitShift));
        result = hashMultiplier * result + (userId != null ? userId.hashCode() : 0);
        result = hashMultiplier * result + (eventData != null ? eventData.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClevertapEvent{"
                + "eventName='" + eventName + '\''
                + ", eventType='" + eventType + '\''
                + ", epochTimestamp=" + epochTimestamp
                + ", userId='" + userId + '\''
                + ", eventData=" + eventData
                + '}';
    }

}
