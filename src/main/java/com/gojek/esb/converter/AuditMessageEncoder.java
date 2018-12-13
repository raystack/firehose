package com.gojek.esb.converter;

import com.gojek.esb.audit.AuditMessage;
import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.consumer.EsbMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AuditMessageEncoder {
    private final AuditMessageBuilder auditMessageBuilder;
    private String auditSource;

    public AuditMessage encode(EsbMessage message) {
        return auditMessageBuilder.build(message.getLogKey(), message.getTopic(), auditSource);
    }
}
