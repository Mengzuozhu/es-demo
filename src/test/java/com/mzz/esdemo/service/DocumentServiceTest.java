package com.mzz.esdemo.service;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.common.TestDataUtil;
import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.common.util.JsonUtil;
import com.mzz.esdemo.model.User;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Zero
 *
 **/
public class DocumentServiceTest {
    private static TestElasticsearchHandler elasticsearchHandler = new TestElasticsearchHandler();
    private DocumentService documentService = new DocumentService(TestElasticsearchHandler.restHighLevelClient());

    @AfterAll
    static void afterAll() {
        elasticsearchHandler.clearIndex();
    }

    @BeforeEach
    void setUp() {
        elasticsearchHandler.upsertDoc();
    }

    @Test
    void getDoc() {
        String id = TestDataUtil.getUser().getId();
        GetResponse response = documentService.getDoc(EsConstant.INDEX_NAME, id);
        assertEquals(id, response.getId());
    }

    @Test
    void createDoc() {
        User user = TestDataUtil.getUser();
        DocWriteResponse response = documentService.createDoc(EsConstant.INDEX_NAME, user.getId(), user);
        assertTrue(RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status()));

    }

    @Test
    void upsertDoc() {
        User user = TestDataUtil.getUser();
        DocWriteResponse response = documentService.upsertDoc(EsConstant.INDEX_NAME, user.getId(), user);
        assertTrue(RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status()));

    }

    @Test
    void upsertDocByBulk() {
        List<User> users = TestElasticsearchHandler.getUsers();
        List<JSONObject> jsons = users.stream()
                .map(JsonUtil::parseToJsonObject)
                .collect(Collectors.toList());
        BulkResponse response = documentService.upsertDocByBulk(EsConstant.INDEX_NAME, jsons);
        assertFalse(response.hasFailures());

    }

    @Test
    void deleteDoc() {
        DocWriteResponse response = documentService.deleteDoc(EsConstant.INDEX_NAME, TestDataUtil.getUser().getId());
        assertEquals(RestStatus.OK, response.status());

    }

    @Test
    void deleteByQuery() {
        TermsQueryBuilder query = QueryBuilders.termsQuery(User.Fields.id, TestDataUtil.getUser().getId());
        BulkByScrollResponse response = documentService.deleteByQuery(EsConstant.INDEX_NAME,
                query.toString());
        assertEquals(0, response.getBulkFailures().size());

    }

    @Test
    void clearIndex() {
        BulkByScrollResponse response = documentService.clearIndex(EsConstant.INDEX_NAME);
        assertEquals(0, response.getBulkFailures().size());

    }

}
