package com.gojek.esb.latestSink.db.field;

public class DBDefaultField implements DBField {
    private Object columnValue;

    public DBDefaultField(Object columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        return columnValue;
    }

    @Override
    public boolean canProcess() {
        return false;
    }
}
