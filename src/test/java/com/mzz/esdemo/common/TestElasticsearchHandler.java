package com.mzz.esdemo.common;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.common.util.EsClientUtil;
import com.mzz.esdemo.common.util.JsonUtil;
import com.mzz.esdemo.model.User;
import com.mzz.esdemo.service.DocumentService;
import com.mzz.esdemo.service.IndexService;
import lombok.Getter;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Test elasticsearch handler.
 *
 * @author zuozhu.meng
 */
public class TestElasticsearchHandler {
    private static final String ES_URL = "http://localhost:9200";
    private static DocumentService documentService;
    @Getter
    private final RestHighLevelClient client;
    private IndexService indexService;
    private String indexName = EsConstant.INDEX_NAME;

    public TestElasticsearchHandler() {
        client = restHighLevelClient();
        documentService = new DocumentService(client);
        indexService = new IndexService(indicesClient());
    }

    public static RestHighLevelClient restHighLevelClient() {
        return EsClientUtil.createRestHighLevelClient(ES_URL);
    }

    public static IndicesClient indicesClient() {
        return EsClientUtil.createRestHighLevelClient(ES_URL).indices();
    }

    public static List<User> getUsers() {
        return Lists.newArrayList(TestDataUtil.getUser(), TestDataUtil.getUser2(), TestDataUtil.getUser3());
    }

    public void upsertDoc() {
        clearIndex();
        List<User> users = getUsers();
        List<JSONObject> jsons = users.stream()
                .map(JsonUtil::parseToJsonObject)
                .collect(Collectors.toList());
        documentService.upsertDocByBulk(indexName, jsons);
        refresh();
    }

    public void upsertDocByBulkForBigData() {
        clearIndex(EsConstant.BIG_INDEX);
        List<JSONObject> jsons = TestDataUtil.generateBigData();
        documentService.upsertDocByBulk(EsConstant.BIG_INDEX, jsons);
        indexService.refresh(EsConstant.BIG_INDEX);
    }

    public void clearIndex() {
        clearIndex(this.indexName);
    }

    public void clearIndex(String indexName) {
        if (indexService.exists(indexName)) {
            documentService.clearIndex(indexName);
            refresh();
        }
    }

    private void refresh() {
        indexService.refresh(indexName);
    }
}
