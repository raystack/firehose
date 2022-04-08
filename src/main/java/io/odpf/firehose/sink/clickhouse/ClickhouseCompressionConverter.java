package io.odpf.firehose.sink.clickhouse;

import com.clickhouse.client.ClickHouseCompression;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class ClickhouseCompressionConverter implements Converter<ClickHouseCompression> {
    @Override
    public ClickHouseCompression convert(Method method, String input) {
        ClickHouseCompression clickHouseCompression = null;

        if (input.equals("lz4")) {
            clickHouseCompression = ClickHouseCompression.LZ4;
        } else if (input.equals("gzip")) {
            clickHouseCompression = ClickHouseCompression.GZIP;
        }
        return clickHouseCompression;
    }
}
