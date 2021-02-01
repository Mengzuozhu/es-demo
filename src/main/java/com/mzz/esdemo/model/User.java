package com.mzz.esdemo.model;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

/**
 * @author Zero
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private String id;
    private String name;
    private String message;
    private Integer height;
    private String degree;
    @JSONField(format = "yyyy-MM-dd")
    private LocalDate updateDate;

}
