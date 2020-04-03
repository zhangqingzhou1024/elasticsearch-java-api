
package com.search.es.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;

/**
 * @author zhangqingzhou
 */
public class JsonMapper {
    private static final Log LOGGER = LogFactory.getLog(JsonMapper.class);
    private ObjectMapper mapper = new ObjectMapper();

    public JsonMapper(Include include) {
        this.mapper.setSerializationInclusion(include);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.mapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
        this.mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        this.mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        this.mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }

    public static JsonMapper buildNormalMapper() {
        return new JsonMapper(Include.ALWAYS);
    }

    public static JsonMapper bulidNormalMapperNameData() {
        JsonMapper jsonMapper = buildNormalMapper();
        jsonMapper.getMapper().setPropertyNamingStrategy(new PropertyNamingStrategy() {
            private static final long serialVersionUID = 2487575658002046742L;

            @Override
            public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
                return "o".equals(defaultName) ? "data" : defaultName;
            }
        });
        return jsonMapper;
    }

    public static JsonMapper buildNonNullMapper() {
        return new JsonMapper(Include.NON_NULL);
    }

    public static JsonMapper buildNonDefaultMapper() {
        return new JsonMapper(Include.NON_DEFAULT);
    }

    public static JsonMapper buildNonEmptyMapper() {
        return new JsonMapper(Include.NON_EMPTY);
    }

    public String toJson(Object object) {
        try {
            String returnString = this.mapper.writeValueAsString(object);
            return returnString != null ? returnString.replace("'", "&acute;") : returnString;
        } catch (IOException var3) {
            LOGGER.warn("toJson(Object) error:" + object, var3);
            return null;
        }
    }

    @JsonCreator
    public <T> T fromJson(String jsonString, Class<T> clazz) {
        if (StringUtils.isEmpty(jsonString)) {
            return null;
        } else {
            try {
                return this.mapper.readValue(jsonString, clazz);
            } catch (IOException var4) {
                LOGGER.warn("fromJson(String, Class<T>) error:" + jsonString, var4);
                return null;
            }
        }
    }

    @JsonCreator
    public <T> T fromJson(String jsonString, JavaType javaType) {
        if (StringUtils.isEmpty(jsonString)) {
            return null;
        } else {
            try {
                return this.mapper.readValue(jsonString, javaType);
            } catch (IOException var4) {
                LOGGER.warn("fromJson(String, JavaType)", var4);
                return null;
            }
        }
    }

    @JsonCreator
    public <T> T fromJson(String jsonString, Type type) {
        if (StringUtils.isEmpty(jsonString)) {
            return null;
        } else {
            try {
                return this.mapper.readValue(jsonString, this.mapper.constructType(type));
            } catch (IOException var4) {
                LOGGER.warn("fromJson(String, Type)", var4);
                return null;
            }
        }
    }

    public JavaType constructParametricType(Class<?> parametrized, Class<?>... parameterClasses) {
        return this.mapper.getTypeFactory().constructParametrizedType(parametrized, parametrized, parameterClasses);
    }

    public <T> T update(T object, String jsonString) {
        try {
            return this.mapper.readerForUpdating(object).readValue(jsonString);
        } catch (JsonProcessingException var4) {
            LOGGER.warn("update json string:" + jsonString + " to object:" + object + " error.", var4);
        } catch (IOException var5) {
            LOGGER.warn("update json string:" + jsonString + " to object:" + object + " error.", var5);
        }

        return null;
    }

    public String toJsonP(String functionName, Object object) {
        return this.toJson(new JSONPObject(functionName, object));
    }

    public void setEnumUseToString(boolean value) {
        this.mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, value);
        this.mapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, value);
    }

    public ObjectMapper getMapper() {
        return this.mapper;
    }
}
