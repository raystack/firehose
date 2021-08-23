package io.odpf.firehose.util;

import java.util.List;

public class ExceptionUtils {
    public static boolean matchCause(Throwable throwable, final Class<? extends Throwable> type) {
        List<Throwable> throwableList = org.apache.commons.lang3.exception.ExceptionUtils.getThrowableList(throwable);
        return throwableList.stream().anyMatch(cause -> cause.getClass().equals(type));
    }
}
