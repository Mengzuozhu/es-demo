package com.mzz.esdemo.service;

import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.model.User;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zuozhu.meng
 * @since 2020/10/21
 **/
class AggregationServiceTest {
    private static TestElasticsearchHandler elasticsearchHandler = new TestElasticsearchHandler();
    private AggregationService aggregationService =
            new AggregationService(TestElasticsearchHandler.restHighLevelClient());

    @AfterAll
    static void afterAll() {
        elasticsearchHandler.clearIndex();
    }

    @BeforeAll
    static void beforeAll() {
        elasticsearchHandler.upsertDoc();
    }

    @Test
    void distinctCount() {
        long distinctCount = aggregationService.distinctCount(EsConstant.INDEX_NAME, "degree.keyword");
        assertEquals(getDegreeCount(), distinctCount);
    }

    @Test
    void minAgg() {
        double minAgg = aggregationService.minAgg(EsConstant.INDEX_NAME, "height");
        Optional<Integer> optional = getHeightStream().min(Comparator.naturalOrder());
        optional.ifPresent(val -> assertEquals(val, (int) minAgg));
    }

    @Test
    void maxAgg() {
        double maxAgg = aggregationService.maxAgg(EsConstant.INDEX_NAME, "height");
        Optional<Integer> optional = getHeightStream().max(Comparator.naturalOrder());
        optional.ifPresent(val -> assertEquals(val, (int) maxAgg));
    }

    @Test
    void avgAgg() {
        double avgAgg = aggregationService.avgAgg(EsConstant.INDEX_NAME, "height");
        System.out.println(avgAgg);
        assertTrue(avgAgg > 0);
    }

    @Test
    void termsAgg() {
        Map<Object, Long> termsAgg = aggregationService.termsCountAgg(EsConstant.INDEX_NAME, "degree.keyword");
        assertEquals(getDegreeCount(), termsAgg.size());
    }

    private Stream<Integer> getHeightStream() {
        return TestElasticsearchHandler.getUsers()
                .stream()
                .map(User::getHeight);
    }

    private long getDegreeCount() {
        return TestElasticsearchHandler.getUsers()
                .stream()
                .map(User::getDegree)
                .distinct()
                .count();
    }
}
