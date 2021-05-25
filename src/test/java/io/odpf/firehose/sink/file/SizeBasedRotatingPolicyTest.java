package io.odpf.firehose.sink.file;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SizeBasedRotatingPolicyTest {

    @Test
    public void shouldNeedRotateWhenSizeExceedTheDefinedLimit() {
        long currentSize = 258;
        SizeBasedRotatingPolicy sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(256);
        sizeBasedRotatingPolicy.setCurrentSize(currentSize);
        boolean needRotate = sizeBasedRotatingPolicy.needRotate();

        assertTrue(needRotate);
    }

    @Test
    public void shouldNotNeedRotateWhenSizeBelowTheLimit() {
        long currentSize = 100;
        SizeBasedRotatingPolicy sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(256);
        sizeBasedRotatingPolicy.setCurrentSize(currentSize);
        boolean needRotate = sizeBasedRotatingPolicy.needRotate();

        assertFalse(needRotate);
    }
}