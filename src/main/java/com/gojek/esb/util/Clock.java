package com.gojek.esb.util;

import java.time.Instant;

public class Clock {
    public Instant now() {
        return Instant.now();
    }
}
