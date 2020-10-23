package com.mzz.esdemo.service;

import com.mzz.esdemo.EsDemoApplication;
import com.mzz.esdemo.common.constant.EsConstant;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author zuozhu.meng
 * @since 2020/10/21
 **/
@SpringBootTest(classes = EsDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SearchServiceTest {
    @Autowired
    private SearchService searchService;

    @Test
    void matchAllQuery() {
        SearchResponse response = searchService.matchAllQuery(EsConstant.INDEX_NAME);
        Assertions.assertEquals(RestStatus.OK, response.status());
    }

    @Test
    void termsQuery() {
        String field = "name";
        String value = "张三";
        SearchResponse response = searchService.termsQuery(EsConstant.INDEX_NAME, field + ".keyword",
                Sets.newHashSet(value));
        Assertions.assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void rangeQuery() {
        String field = "height";
        int value = 170;
        SearchResponse response = searchService.rangeQuery(EsConstant.INDEX_NAME, field, value, 180);
        Assertions.assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void matchQuery() {
        String field = "name";
        String value = "张三";
        SearchResponse response = searchService.matchQuery(EsConstant.INDEX_NAME, field, value);
        Assertions.assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void matchPhraseQuery() {
        String field = "message";
        String value = "Elasticsearch demo";
        SearchResponse response = searchService.matchPhraseQuery(EsConstant.INDEX_NAME, field, value);
        Assertions.assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void queryStringQuery() {
        String field = "message";
        String value = "demo";
        SearchResponse response = searchService.queryStringQuery(EsConstant.INDEX_NAME, value);
        Assertions.assertTrue(response.getHits().getHits()[0].getSourceAsMap().get(field).toString().contains(value));
    }

    @Test
    void queryByJson() {
        String field = "name";
        String value = "张三";
        // String json = QueryBuilders.matchQuery(field, value).toString();
        String json = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(field, value))
                .toString();
        SearchResponse response = searchService.queryByJson(EsConstant.INDEX_NAME, json);
        Assertions.assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

}
