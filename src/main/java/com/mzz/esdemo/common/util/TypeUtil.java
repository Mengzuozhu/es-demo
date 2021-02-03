package com.mzz.esdemo.common.util;

/**
 * @author Zero
 **/
public class TypeUtil {
    /**
     * 转换类型，若为null或转换失败，则返回null
     *
     * @param <T> the type parameter
     * @param obj the obj
     * @param cls the cls
     * @return the t
     */
    public static <T> T convertAs(Object obj, Class<T> cls) {
        if (cls.isInstance(obj)) {
            return cls.cast(obj);
        }
        return null;
    }

    /**
     * 是否任意一个参数为空
     *
     * @param objects the objects
     * @return the boolean
     */
    public static boolean isAnyNull(Object... objects) {
        if (objects == null) {
            return true;
        }
        for (Object obj : objects) {
            if (obj == null) {
                return true;
            }
        }
        return false;
    }
}
