package com.mzz.esdemo.service;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.EsDemoApplication;
import com.mzz.esdemo.common.TestDataUtil;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.common.util.JsonUtil;
import com.mzz.esdemo.model.User;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zuozhu.meng
 * @since 2020/10/20
 **/
@SpringBootTest(classes = EsDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DocumentServiceTest {

    @Autowired
    private DocumentService documentService;
    @Autowired
    private IndexService indexService;

    @Test
    public void upsertDocByBulkForBigData() {
        List<JSONObject> jsons = TestDataUtil.generateBigData();
        BulkResponse response = documentService.upsertDocByBulk(EsConstant.BIG_INDEX, jsons);
        Assertions.assertFalse(response.hasFailures());
    }

    @BeforeEach
    void setUp() {
        User user = TestDataUtil.getUser();
        documentService.upsertDoc(EsConstant.INDEX_NAME, user.getId(), user);
        indexService.refresh(EsConstant.INDEX_NAME);
    }

    @Test
    void getDoc() {
        String id = TestDataUtil.getUser().getId();
        GetResponse response = documentService.getDoc(EsConstant.INDEX_NAME, id);
        Assertions.assertEquals(id, response.getId());
    }

    @Test
    void createDoc() {
        User user = TestDataUtil.getUser();
        DocWriteResponse response = documentService.createDoc(EsConstant.INDEX_NAME, user.getId(), user);
        Assertions.assertTrue(RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status()));
        System.out.println(response.toString());
    }

    @Test
    void upsertDoc() {
        User user = TestDataUtil.getUser();
        DocWriteResponse response = documentService.upsertDoc(EsConstant.INDEX_NAME, user.getId(), user);
        Assertions.assertTrue(RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status()));
        System.out.println(response.toString());
    }

    @Test
    void upsertDocByBulk() {
        List<User> users = Lists.newArrayList(TestDataUtil.getUser(), TestDataUtil.getUser2(), TestDataUtil.getUser3());
        List<JSONObject> jsons = users.stream()
                .map(JsonUtil::parseToJsonObject)
                .collect(Collectors.toList());
        BulkResponse response = documentService.upsertDocByBulk(EsConstant.INDEX_NAME, jsons);
        Assertions.assertFalse(response.hasFailures());
        System.out.println(response.toString());
    }

    @Test
    void deleteDoc() {
        DocWriteResponse response = documentService.deleteDoc(EsConstant.INDEX_NAME, TestDataUtil.getUser().getId());
        Assertions.assertEquals(RestStatus.OK, response.status());
        System.out.println(response.toString());
    }

    @Test
    void deleteByQuery() {
        TermsQueryBuilder query = QueryBuilders.termsQuery("id", TestDataUtil.getUser().getId());
        BulkByScrollResponse response = documentService.deleteByQuery(EsConstant.INDEX_NAME,
                query.toString());
        Assertions.assertEquals(0, response.getBulkFailures().size());
        System.out.println(response.toString());
    }

    @Test
    void clearIndex() {
        BulkByScrollResponse response = documentService.clearIndex(EsConstant.INDEX_NAME);
        Assertions.assertEquals(0, response.getBulkFailures().size());
        System.out.println(response.toString());
    }

}
