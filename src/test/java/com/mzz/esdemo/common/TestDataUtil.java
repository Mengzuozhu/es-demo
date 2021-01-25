package com.mzz.esdemo.common;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.common.util.JsonUtil;
import com.mzz.esdemo.model.User;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author zuozhu.meng
 * @since 2020/10/23
 **/
public class TestDataUtil {

    public static final int BIG_SIZE = 200;

    public static User getUser() {
        return User.builder()
                .id("1")
                .name("张三")
                .message("Elasticsearch demo")
                .height(170)
                .degree("硕士")
                .updateDate(getUpdateDate())
                .build();
    }

    public static User getUser2() {
        return User.builder()
                .id("2")
                .name("李四")
                .message("Elasticsearch demo")
                .height(175)
                .degree("本科")
                .updateDate(getUpdateDate())
                .build();
    }

    public static User getUser3() {
        return User.builder()
                .id("3")
                .name("王五")
                .message("Es demo")
                .height(170)
                .degree("本科")
                .updateDate(getUpdateDate())
                .build();
    }

    public static List<JSONObject> generateBigData() {
        return IntStream.range(0, BIG_SIZE)
                .mapToObj(i -> {
                    User user = getUser();
                    user.setId(String.valueOf(i));
                    user.setName("name_" + i);
                    return JsonUtil.parseToJsonObject(user);
                })
                .collect(Collectors.toList());
    }

    private static LocalDate getUpdateDate() {
        return Instant.now().atZone(ZoneId.systemDefault()).toLocalDate();
    }
}
