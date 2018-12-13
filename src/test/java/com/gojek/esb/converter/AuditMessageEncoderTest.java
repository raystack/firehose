package com.gojek.esb.converter;

import com.gojek.esb.audit.AuditMessage;
import com.gojek.esb.audit.AuditMessageBuilder;
import com.gojek.esb.consumer.EsbMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class AuditMessageEncoderTest {

    @Mock
    private AuditMessageBuilder auditMessageBuilder;

    private String auditSource = "auditSource";
    private AuditMessage resultedAuditMessage;

    @Captor
    private ArgumentCaptor<String> sourceCaptor;

    @Test
    public void shouldBuildMultipleMessagesWithSameSourceConfig() throws Exception {

        AuditMessageEncoder auditMessageEncoder = new AuditMessageEncoder(auditMessageBuilder, auditSource);

        auditMessageEncoder.encode(new EsbMessage("key".getBytes(), "msg".getBytes(), "topic1", 0, 100));
        auditMessageEncoder.encode(new EsbMessage("key1".getBytes(), "msg1".getBytes(), "topic2", 0, 100));

        verify(auditMessageBuilder, times(2)).build(any(), any(), sourceCaptor.capture());
        assertThat(sourceCaptor.getAllValues(), is(asList(auditSource, auditSource)));
    }
}
