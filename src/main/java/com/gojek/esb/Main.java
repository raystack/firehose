package com.gojek.esb;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.config.ApplicationConfiguration;
import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.consumer.EsbConsumer;
import com.gojek.esb.factory.KafkaFactory;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;


public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        ApplicationConfiguration environment = ConfigFactory.create(ApplicationConfiguration.class, System.getenv());
        KafkaConsumerConfig config = new KafkaConsumerConfig(environment.getKafkaAddress(),
                environment.getConsumerGroupId(),
                Pattern.compile("GO_RIDE-booking-log")
        );

        KafkaFactory factory = new KafkaFactory<BookingLogKey, BookingLogMessage>();
        Predicate<BookingLogMessage> nullPredicate = bookingLogMessage -> true;

        EsbConsumer<BookingLogKey, BookingLogMessage> consumer = factory.createConsumer(config, BookingLogMessage.parser(), nullPredicate);

        logger.info("Starting to pull");
        List<BookingLogMessage> bookingLogMessages = consumer.readMessages();
        logger.info("Pulled something");
        if (!bookingLogMessages.isEmpty()) {
            logger.info("{}", bookingLogMessages);
        }
    }

}
