package com.gojek.esb.sink;

public interface SinkCommand {
    void execute() throws SinkCommandExecutionException;
}
