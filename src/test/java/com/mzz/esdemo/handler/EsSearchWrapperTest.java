package com.mzz.esdemo.handler;

import com.mzz.esdemo.common.constant.EsConstant;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * 注：使用方法{@link com.mzz.esdemo.service.DocumentServiceTest#upsertDocByBulkForBigData()}生成测试数据
 *
 * @author zuozhu.meng
 * @since 2020/10/22
 **/
class EsSearchWrapperTest {
    private EsSearchWrapper esSearchWrapper = EsSearchWrapper.buildInstance("http://localhost:9200");

    @Test
    void searchByPageForHit() {
        int pageSize = 100;
        searchForHit((searchRequest, count) -> esSearchWrapper.searchByPageForHit(searchRequest, pageSize,
                searchHit -> count.addAndGet(1)));
    }

    @Test
    void searchByPageForResponse() {
        int pageSize = 100;
        searchForResponse((searchRequest, allCount, responseCount) -> {
            esSearchWrapper.searchByPageForResponse(searchRequest, pageSize, response -> {
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
            esSearchWrapper.searchByScrollForResponse(searchRequest, new TimeValue(10000), response -> {
                allCount.addAndGet(response.getHits().getHits().length);
                responseCount.addAndGet(1);
            });
        });
    }

    private void searchForResponse(TriConsumer<SearchRequest, AtomicInteger, AtomicInteger> triConsumer) {
        int batchSize = 100;
        SearchRequest searchRequest = getMatchAllRequest();
        searchRequest.source().size(batchSize);
        AtomicInteger allCount = new AtomicInteger();
        AtomicInteger responseCount = new AtomicInteger();
        triConsumer.apply(searchRequest, allCount, responseCount);
        // 只有100次SearchResponse，每个Response的记录数为批量大小100
        Assertions.assertEquals(100, responseCount.get());
        Assertions.assertEquals(10000, allCount.get());
    }

    private void searchForHit(BiConsumer<SearchRequest, AtomicLong> consumer) {
        int batchSize = 100;
        SearchRequest searchRequest = getMatchAllRequest();
        searchRequest.source().size(batchSize);
        AtomicLong count = new AtomicLong();
        consumer.accept(searchRequest, count);
        Assertions.assertEquals(10000, count.get());
    }

    private SearchRequest getMatchAllRequest() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        return new SearchRequest(EsConstant.BIG_INDEX)
                .source(sourceBuilder);
    }
}
