package com.mzz.esdemo.service;

import com.mzz.esdemo.common.TestDataUtil;
import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.model.User;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Zero
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
        String field = User.Fields.name;
        String value = TestDataUtil.getUser().getName();
        SearchResponse response = searchService.termsQuery(EsConstant.INDEX_NAME, field + ".keyword",
                Sets.newHashSet(value));
        assertEquals(value, getFirstSourceAsMap(response).get(field));
    }

    @Test
    void idsQuery() {
        String id = TestDataUtil.getUser().getId();
        SearchResponse response = searchService.idsQuery(EsConstant.INDEX_NAME, id);
        assertEquals(id, getFirstHit(response).getId());
    }

    @Test
    void rangeQuery() {
        String field = User.Fields.height;
        int value = TestDataUtil.getUser().getHeight();
        SearchResponse response = searchService.rangeQuery(EsConstant.INDEX_NAME, field, value, value + 10);
        assertEquals(value, getFirstSourceAsMap(response).get(field));
    }

    @Test
    void matchQuery() {
        String field = User.Fields.name;
        String value = TestDataUtil.getUser().getName();
        SearchResponse response = searchService.matchQuery(EsConstant.INDEX_NAME, field, value);
        assertEquals(value, getFirstSourceAsMap(response).get(field));
    }

    @Test
    void matchPhraseQuery() {
        String field = User.Fields.message;
        String value = TestDataUtil.getUser().getMessage();
        SearchResponse response = searchService.matchPhraseQuery(EsConstant.INDEX_NAME, field, value);
        assertEquals(value, getFirstSourceAsMap(response).get(field));
    }

    @Test
    void queryStringQuery() {
        String field = User.Fields.message;
        String value = "demo";
        SearchResponse response = searchService.queryStringQuery(EsConstant.INDEX_NAME, value);
        assertTrue(getFirstSourceAsMap(response).get(field).toString().contains(value));
    }

    @Test
    void queryByJson() {
        String field = User.Fields.name;
        String value = TestDataUtil.getUser().getName();
        String json = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(field, value))
                .toString();
        SearchResponse response = searchService.queryByJson(EsConstant.INDEX_NAME, json);
        assertEquals(value, getFirstSourceAsMap(response).get(field));
    }

    private Map<String, Object> getFirstSourceAsMap(SearchResponse response) {
        return getFirstHit(response).getSourceAsMap();
    }

    private SearchHit getFirstHit(SearchResponse response) {
        return response.getHits().getHits()[0];
    }

}
