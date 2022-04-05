package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseCompression;
import io.odpf.firehose.exception.DeserializerException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.SQLException;

@RunWith(MockitoJUnitRunner.class)
public class ClickhouseCompressionConverterTest {

    @Test
    public void shouldReturnCorrectCompression() throws IOException, DeserializerException, SQLException {
        ClickhouseCompressionConverter clickhouseCompressionConverter = new ClickhouseCompressionConverter();
        Assert.assertEquals(ClickHouseCompression.LZ4,clickhouseCompressionConverter.convert(null,"lz4"));
        Assert.assertEquals(ClickHouseCompression.GZIP,clickhouseCompressionConverter.convert(null,"gzip"));
    }
}