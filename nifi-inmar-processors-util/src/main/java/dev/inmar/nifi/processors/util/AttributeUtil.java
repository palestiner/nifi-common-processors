package dev.inmar.nifi.processors.util;

import org.apache.commons.collections4.MapUtils;

import java.util.Map;

public final class AttributeUtil {

    private AttributeUtil() {}

    public static void putAllUnique(Map<String, String> resultAttributes, Map<String, String> newAttributes) {
        if (resultAttributes == null || MapUtils.isEmpty(newAttributes)) {
            return;
        }

        for (String newKey : newAttributes.keySet()) {
            if (!resultAttributes.containsKey(newKey)) {
                resultAttributes.put(newKey, newAttributes.get(newKey));
            }
        }
    }

}
