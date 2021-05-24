package io.odpf.firehose.sink.file;

/**
 * RotatingFilePolicy is state manager for rotating file writing behavior
 */
public interface RotatingFilePolicy {
    boolean needRotate();
    // TODO: 21/05/21 add update method
}
