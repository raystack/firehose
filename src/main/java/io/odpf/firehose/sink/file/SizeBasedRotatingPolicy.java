package io.odpf.firehose.sink.file;

public class SizeBasedRotatingPolicy implements RotatingFilePolicy {

    private final long size;
    private long currentSize;

    /**
     * @param size is data size in bytes
     */
    public SizeBasedRotatingPolicy(long size) {
        this.size = size;
    }

    public void add(long size){
        this.currentSize += size;
    }

    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }

    @Override
    public boolean needRotate() {
        return currentSize > size;
    }
}
