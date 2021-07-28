package io.odpf.firehose.sink.common;

import com.google.protobuf.DynamicMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoUtil {
    public static boolean isUnknownFieldExist(DynamicMessage dynamicMessage) {
        if (dynamicMessage == null) {
            return false;
        }
        List<DynamicMessage> dynamicMessageFields = new LinkedList<>();
        collectNestedFields(dynamicMessage, dynamicMessageFields);
        List<DynamicMessage> messageWithUnknownFields = getMessageWithUnknownFields(dynamicMessageFields);
        return messageWithUnknownFields.size() > 0;
    }

    private static void collectNestedFields(DynamicMessage root, List<DynamicMessage> accumulator) {
        List<DynamicMessage> nestedChild = root.getAllFields().values().stream()
                .filter(v -> v instanceof DynamicMessage)
                .map(o -> (DynamicMessage) o)
                .collect(Collectors.toList());

        nestedChild.forEach(m -> collectNestedFields(m, accumulator));
        accumulator.add(root);
    }

    private static List<DynamicMessage> getMessageWithUnknownFields(List<DynamicMessage> messages) {
        return messages.stream().filter(message -> message.getUnknownFields().asMap().size() > 0).collect(Collectors.toList());
    }
}
