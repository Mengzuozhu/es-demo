package com.mzz.esdemo.handler;

import com.mzz.esdemo.common.TestDataUtil;
import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zuozhu.meng
 * @since 2020/10/22
 **/
class EsSearchWrapperTest {
    private static final int PAGE_SIZE = 100;
    private static TestElasticsearchHandler elasticsearchHandler = new TestElasticsearchHandler();
    private EsSearchWrapper esSearchWrapper = EsSearchWrapper.of(elasticsearchHandler.getClient());

    @AfterAll
    static void afterAll() {
        elasticsearchHandler.clearIndex(EsConstant.BIG_INDEX);
    }

    @BeforeAll
    static void beforeAll() {
        elasticsearchHandler.upsertDocByBulkForBigData();
    }

    @Test
    void searchByPageForHit() {
        searchForHit((searchRequest, count) -> esSearchWrapper.searchByPageForHit(searchRequest, PAGE_SIZE,
                searchHit -> count.addAndGet(1)));
    }

    @Test
    void searchByPageForResponse() {
        searchForResponse((searchRequest, allCount, responseCount) -> {
            esSearchWrapper.searchByPageForResponse(searchRequest, PAGE_SIZE, response -> {
                allCount.addAndGet(response.getHits().getHits().length);
                responseCount.addAndGet(1);
            });
        });
    }

    @Test
    void searchAfterForHit() {
        searchForHit((searchRequest, count) -> esSearchWrapper.searchAfterForHit(searchRequest,
                searchHit -> count.addAndGet(1)));
    }

    @Test
    void searchAfterForResponse() {
        searchForResponse((searchRequest, allCount, responseCount) -> {
            esSearchWrapper.searchAfterForResponse(searchRequest, response -> {
                allCount.addAndGet(response.getHits().getHits().length);
                responseCount.addAndGet(1);
            });
        });
    }

    @Test
    void searchByScrollForHit() {
        searchForHit((searchRequest, count) -> esSearchWrapper.searchByScrollForHit(searchRequest,
                searchHit -> count.addAndGet(1)));
    }

    @Test
    void searchByScrollForResponse() {
        searchForResponse((searchRequest, allCount, responseCount) -> {
            esSearchWrapper.searchByScrollForResponse(searchRequest, response -> {
                allCount.addAndGet(response.getHits().getHits().length);
                responseCount.addAndGet(1);
            });
        });
    }

    @Test
    void searchByScrollForResponseWhenCustomKeepAlive() {
        searchForResponse((searchRequest, allCount, responseCount) -> {
            esSearchWrapper.searchByScrollForResponse(searchRequest, new TimeValue(TestDataUtil.BIG_SIZE), response -> {
                allCount.addAndGet(response.getHits().getHits().length);
                responseCount.addAndGet(1);
            });
        });
    }

    private void searchForResponse(TriConsumer<SearchRequest, AtomicInteger, AtomicInteger> triConsumer) {
        SearchRequest searchRequest = getMatchAllRequest();
        searchRequest.source().size(PAGE_SIZE);
        AtomicInteger allCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        triConsumer.apply(searchRequest, allCount, responseCount);
        // 只有100次SearchResponse，每个Response的记录数为批量大小100
        int pageCount = (int) Math.ceil(TestDataUtil.BIG_SIZE * 1.0 / PAGE_SIZE);
        assertEquals(pageCount, responseCount.get());
        assertEquals(TestDataUtil.BIG_SIZE, allCount.get());
    }

    private void searchForHit(BiConsumer<SearchRequest, AtomicLong> consumer) {
        SearchRequest searchRequest = getMatchAllRequest();
        searchRequest.source()
                .size(PAGE_SIZE);
        AtomicLong count = new AtomicLong();
        consumer.accept(searchRequest, count);
        assertEquals(TestDataUtil.BIG_SIZE, count.get());
    }

    private SearchRequest getMatchAllRequest() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        return new SearchRequest(EsConstant.BIG_INDEX)
                .source(sourceBuilder);
    }
}
