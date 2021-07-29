package io.odpf.firehose.sink.common;

import com.google.protobuf.DynamicMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoUtil {
    public static boolean isUnknownFieldExist(DynamicMessage root) {
        if (root == null) {
            return false;
        }
        List<DynamicMessage> dynamicMessageFields = new LinkedList<>();
        collectNestedFields(root, dynamicMessageFields);
        List<DynamicMessage> messageWithUnknownFields = getMessageWithUnknownFields(dynamicMessageFields);
        return messageWithUnknownFields.size() > 0;
    }

    private static void collectNestedFields(DynamicMessage node, List<DynamicMessage> accumulator) {
        List<DynamicMessage> nestedChildNodes = node.getAllFields().values().stream()
                .filter(field -> field instanceof DynamicMessage)
                .map(field -> (DynamicMessage) field)
                .collect(Collectors.toList());
        nestedChildNodes.forEach(n -> collectNestedFields(n, accumulator));

        accumulator.add(node);
    }

    private static List<DynamicMessage> getMessageWithUnknownFields(List<DynamicMessage> messages) {
        return messages.stream().filter(message -> message.getUnknownFields().asMap().size() > 0).collect(Collectors.toList());
    }
}
