package com.gojek.esb.sink.http.request.entity;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.StringEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.MockitoAnnotations.initMocks;

public class EntityBuilderTest {

    private String bodyContent;

    @Before
    public void setUp() {
        initMocks(this);
        bodyContent = "dummyContent";
    }

    @Test
    public void shouldCreateStringEntity() throws IOException {
        EntityBuilder entityBuilder = new EntityBuilder();

        StringEntity stringEntity = entityBuilder.buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("dummyContent", new String(bytes));
    }

    @Test
    public void shouldWrapEntityIfSet() throws IOException {
        EntityBuilder entityBuilder = new EntityBuilder();

        StringEntity stringEntity = entityBuilder.setWrapping(true).buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("[dummyContent]", new String(bytes));
    }

    @Test
    public void shouldNotWrapEntityIfNotSet() throws IOException {
        EntityBuilder entityBuilder = new EntityBuilder();

        StringEntity stringEntity = entityBuilder.setWrapping(false).buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("dummyContent", new String(bytes));
    }
}
