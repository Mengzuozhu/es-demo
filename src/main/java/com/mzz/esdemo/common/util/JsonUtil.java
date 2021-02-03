package com.mzz.esdemo.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * json工具
 *
 * @author Zero
 **/
public class JsonUtil {

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
     * Value to json object.
     *
     * @param obj the obj
     * @return the json object
     */
    public static JSONObject parseToJsonObject(Object obj) {
        return parseObject(obj, JSONObject.class);
    }

}
