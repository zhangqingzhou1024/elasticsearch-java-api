
package com.search.es.util;

import com.fasterxml.jackson.databind.JavaType;

import java.util.List;
import java.util.Map;

/**
 * josn 工具类
 *
 * @author zhangqingzhou
 */
public class JsonHelper {
    private static JsonMapper jsonMapper;

    public JsonHelper() {
    }

    public static void setJsonMapper(JsonMapper jsonMapper) {
        JsonHelper.jsonMapper = jsonMapper;
    }

    private static JsonMapper getJsonMapper() {
        return null != jsonMapper ? jsonMapper : JsonMapper.buildNonNullMapper();
    }

    public static String toJson(Object obj) {
        return getJsonMapper().toJson(obj);
    }

    public static <T> T toBean(String json, Class<T> responseType) {
        return getJsonMapper().fromJson(json, responseType);
    }

    public static Object toCollectionBean(String json, Class<?> collectionClass, Class<?>... elementClasses) {
        JavaType javaType = getJsonMapper().constructParametricType(collectionClass, elementClasses);
        return getJsonMapper().fromJson(json, javaType);
    }

    public static <T> List<T> toList(String json, Class<T> parameterClass) {
        JavaType javaType = getJsonMapper().constructParametricType(List.class, new Class[]{parameterClass});
        return (List) getJsonMapper().fromJson(json, javaType);
    }

    public static <K, V> Map<K, V> toMap(String json, Class<K> keyClass, Class<V> valueClass) {
        JavaType javaType = getJsonMapper().constructParametricType(Map.class, new Class[]{keyClass, valueClass});
        return (Map) getJsonMapper().fromJson(json, javaType);
    }


}
