package com.mzz.esdemo.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * json工具
 *
 * @author zuozhu.meng
 **/
public class JsonUtil {

    /**
     * 转换为JSONArray
     * <p>转换失败，则添加到JSONArray<p/>
     *
     * @param obj the obj
     * @return the json array
     */
    public static JSONArray addOrToJsonArray(Object obj) {
        if (obj == null) {
            return null;
        }
        JSONArray valueArray = TypeUtil.convertAs(obj, JSONArray.class);
        return valueArray != null ? valueArray : newJsonArray(obj);
    }

    /**
     * 创建包含给定对象的JSONArray
     *
     * @param objs the objs
     * @return the json array
     */
    public static JSONArray newJsonArray(Object... objs) {
        if (objs == null) {
            return new JSONArray();
        }
        JSONArray jsonArray = new JSONArray(objs.length);
        Collections.addAll(jsonArray, objs);
        return jsonArray;
    }

    /**
     * New json object.
     *
     * @param key   the key
     * @param value the value
     * @return the json object
     */
    public static JSONObject newJsonObject(String key, Object value) {
        JSONObject json = new JSONObject();
        json.put(key, value);
        return json;
    }

    /**
     * Foreach json array.
     *
     * @param jsonArray the json array
     * @param action    the action
     */
    public static void foreachJsonArray(JSONArray jsonArray, Consumer<? super JSONObject> action) {
        if (CollectionUtils.isEmpty(jsonArray)) {
            return;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject json = jsonArray.getJSONObject(i);
            if (json != null) {
                action.accept(json);
            }
        }
    }

    /**
     * Deep copy list by json list.
     *
     * @param <T>  the type parameter
     * @param list the list
     * @param cls  the cls
     * @return the list
     */
    public static <T> List<T> deepCopyListByJson(List<T> list, Class<T> cls) {
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        return JSON.parseArray(JSON.toJSONString(list), cls);
    }

    /**
     * Parse array list.
     *
     * @param <T> the type parameter
     * @param obj the obj
     * @param cls the cls
     * @return the list
     */
    public static <T> List<T> parseArray(Object obj, Class<T> cls) {
        return obj == null ? new ArrayList<>() : JSON.parseArray(JSON.toJSONString(obj), cls);
    }

    /**
     * Parse object t.
     *
     * @param <T> the type parameter
     * @param obj the obj
     * @param cls the cls
     * @return the t
     */
    public static <T> T parseObject(Object obj, Class<T> cls) {
        return obj == null ? null : JSON.parseObject(JSON.toJSONString(obj), cls);
    }

    /**
     * Parse object t.
     *
     * @param <T>     the type parameter
     * @param obj     the obj
     * @param cls     the cls
     * @param feature the feature
     * @return the t
     */
    public static <T> T parseObject(Object obj, Class<T> cls, SerializerFeature feature) {
        return obj == null ? null : JSON.parseObject(JSON.toJSONString(obj, feature), cls);
    }

    /**
     * Value to json object.
     *
     * @param obj the obj
     * @return the json object
     */
    public static JSONObject parseToJsonObject(Object obj) {
        return parseObject(obj, JSONObject.class);
    }

    /**
     * To json string.
     *
     * @param obj the obj
     * @return the string
     */
    public static String toJsonString(Object obj) {
        return JSON.toJSONString(obj);
    }
}
