package com.gojek.esb.sink.db.field.message;

import com.gojek.esb.sink.db.field.DBField;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class DBDefaultMessageField implements DBField {
    private Object columnValue;
    private JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();

    public DBDefaultMessageField(Object columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        try {
            columnValue = this.jsonPrinter.print((Message) columnValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return columnValue;
    }

    @Override
    public boolean canProcess() {
        return columnValue instanceof Message;
    }
}
