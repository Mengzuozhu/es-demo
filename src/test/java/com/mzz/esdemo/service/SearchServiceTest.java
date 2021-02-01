package com.mzz.esdemo.service;

import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Zero
 * @since 2020/10/21
 **/
class SearchServiceTest {
    private static SearchService searchService;
    private static TestElasticsearchHandler elasticsearchHandler = new TestElasticsearchHandler();

    @AfterAll
    static void afterAll() {
        elasticsearchHandler.clearIndex();
    }

    @BeforeAll
    static void beforeAll() {
        elasticsearchHandler.upsertDoc();
        searchService = new SearchService(elasticsearchHandler.getClient());
    }

    @Test
    void matchAllQuery() {
        SearchResponse response = searchService.matchAllQuery(EsConstant.INDEX_NAME);
        assertEquals(TestElasticsearchHandler.getUsers().size(), response.getHits().getHits().length);
    }

    @Test
    void termsQuery() {
        String field = "name";
        String value = "张三";
        SearchResponse response = searchService.termsQuery(EsConstant.INDEX_NAME, field + ".keyword",
                Sets.newHashSet(value));
        assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void rangeQuery() {
        String field = "height";
        int value = 170;
        SearchResponse response = searchService.rangeQuery(EsConstant.INDEX_NAME, field, value, 180);
        assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void matchQuery() {
        String field = "name";
        String value = "张三";
        SearchResponse response = searchService.matchQuery(EsConstant.INDEX_NAME, field, value);
        assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void matchPhraseQuery() {
        String field = "message";
        String value = "Elasticsearch demo";
        SearchResponse response = searchService.matchPhraseQuery(EsConstant.INDEX_NAME, field, value);
        assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

    @Test
    void queryStringQuery() {
        String field = "message";
        String value = "demo";
        SearchResponse response = searchService.queryStringQuery(EsConstant.INDEX_NAME, value);
        assertTrue(response.getHits().getHits()[0].getSourceAsMap().get(field).toString().contains(value));
    }

    @Test
    void queryByJson() {
        String field = "name";
        String value = "张三";
        String json = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(field, value))
                .toString();
        SearchResponse response = searchService.queryByJson(EsConstant.INDEX_NAME, json);
        assertEquals(value, response.getHits().getHits()[0].getSourceAsMap().get(field));
    }

}
