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
    private static final String esUrl = "http://localhost:9200";
    private static DocumentService documentService;
    @Getter
    private final RestHighLevelClient client;
    private IndexService indexService;

    public TestElasticsearchHandler() {
        client = restHighLevelClient();
        documentService = new DocumentService(client);
        indexService = new IndexService(indicesClient());
    }

    public static RestHighLevelClient restHighLevelClient() {
        return EsClientUtil.createRestHighLevelClient(esUrl);
    }

    public static IndicesClient indicesClient() {
        return EsClientUtil.createRestHighLevelClient(esUrl).indices();
    }

    public void upsertDoc() {
        clearIndex();
        List<User> users = Lists.newArrayList(TestDataUtil.getUser(), TestDataUtil.getUser2(), TestDataUtil.getUser3());
        List<JSONObject> jsons = users.stream()
                .map(JsonUtil::parseToJsonObject)
                .collect(Collectors.toList());
        documentService.upsertDocByBulk(EsConstant.INDEX_NAME, jsons);
        refresh();
    }

    public void clearIndex() {
        documentService.clearIndex(EsConstant.INDEX_NAME);
        refresh();
    }

    private void refresh() {
        indexService.refresh(EsConstant.INDEX_NAME);
    }
}
