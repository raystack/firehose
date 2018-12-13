package com.gojek.esb.config;

public class DisabledAuditConfig implements AuditConfig {

    @Override
    public Boolean isAuditEnabled() {
        return false;
    }

    @Override
    public String statsDHost() {
        return null;
    }

    @Override
    public int statsDPort() {
        return 0;
    }

    @Override
    public String source() {
        return null;
    }
}
