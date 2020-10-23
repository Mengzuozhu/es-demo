package com.mzz.esdemo.service;

import com.mzz.esdemo.EsDemoApplication;
import com.mzz.esdemo.common.constant.EsConstant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

/**
 * @author zuozhu.meng
 * @since 2020/10/21
 **/
@SpringBootTest(classes = EsDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AggregationServiceTest {
    @Autowired
    private AggregationService aggregationService;

    @Test
    void distinctCount() {
        long distinctCount = aggregationService.distinctCount(EsConstant.INDEX_NAME, "degree.keyword");
        System.out.println(distinctCount);
    }

    @Test
    void minAgg() {
        double maxAgg = aggregationService.minAgg(EsConstant.INDEX_NAME, "height");
        System.out.println(maxAgg);
    }

    @Test
    void maxAgg() {
        double maxAgg = aggregationService.maxAgg(EsConstant.INDEX_NAME, "height");
        System.out.println(maxAgg);
    }

    @Test
    void avgAgg() {
        double maxAgg = aggregationService.avgAgg(EsConstant.INDEX_NAME, "height");
        System.out.println(maxAgg);
    }

    @Test
    void termsAgg() {
        Map<Object, Long> termsAgg = aggregationService.termsCountAgg(EsConstant.INDEX_NAME, "degree.keyword");
        System.out.println(termsAgg.toString());
    }
}
