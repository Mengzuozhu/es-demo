package com.mzz.esdemo.service;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.common.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * The type Document service.
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html">
 *
 * @author Zero
 * @since 2020 /10/20
 */
@Service
@RequiredArgsConstructor
public class DocumentService {

    private final RestHighLevelClient restHighLevelClient;

    /**
     * Create doc.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the doc write response
     */
    @SneakyThrows
    public DocWriteResponse createDoc(String index, String id, Object source) {
        IndexRequest indexRequest = new IndexRequest(index)
                .id(id)
                .source(JsonUtil.toJsonString(source), XContentType.JSON);
        return restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    /**
     * Upsert doc.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the doc write response
     */
    @SneakyThrows
    public DocWriteResponse upsertDoc(String index, String id, Object source) {
        String jsonString = JsonUtil.toJsonString(source);
        UpdateRequest updateRequest = new UpdateRequest(index, id)
                .doc(jsonString, XContentType.JSON)
                .upsert(jsonString, XContentType.JSON);
        return restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
    }

    /**
     * Upsert doc by bulk.
     *
     * @param index   the index
     * @param sources the sources
     * @return the bulk response
     */
    @SneakyThrows
    public BulkResponse upsertDocByBulk(String index, List<JSONObject> sources) {
        BulkRequest bulkRequest = new BulkRequest(index);
        for (JSONObject source : sources) {
            String jsonString = JsonUtil.toJsonString(source);
            UpdateRequest updateRequest = new UpdateRequest(index, source.getString(EsConstant.ID))
                    .doc(jsonString, XContentType.JSON)
                    .upsert(jsonString, XContentType.JSON);
            bulkRequest.add(updateRequest);
        }
        return restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    /**
     * Gets doc.
     *
     * @param index the index
     * @param id    the id
     * @return the doc
     */
    @SneakyThrows
    public GetResponse getDoc(String index, String id) {
        GetRequest deleteRequest = new GetRequest(index, id);
        return restHighLevelClient.get(deleteRequest, RequestOptions.DEFAULT);
    }

    /**
     * Delete doc.
     *
     * @param index the index
     * @param id    the id
     * @return the doc write response
     */
    @SneakyThrows
    public DocWriteResponse deleteDoc(String index, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        return restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    /**
     * Delete by query.
     *
     * @param index     the index
     * @param queryJson the query json
     * @return the bulk by scroll response
     */
    @SneakyThrows
    public BulkByScrollResponse deleteByQuery(String index, String queryJson) {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(index)
                .setQuery(QueryBuilders.wrapperQuery(queryJson));
        return restHighLevelClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    }

    /**
     * Clear index.
     *
     * @param index the index
     * @return the bulk by scroll response
     */
    @SneakyThrows
    public BulkByScrollResponse clearIndex(String index) {
        return deleteByQuery(index, QueryBuilders.matchAllQuery().toString());
    }

    /**
     * Reindex.
     *
     * @param sourceIndex the source index
     * @param destIndex   the dest index
     * @return the bulk by scroll response
     */
    @SneakyThrows
    public BulkByScrollResponse reindex(String sourceIndex, String destIndex) {
        return restHighLevelClient.reindex(new ReindexRequest()
                .setSourceIndices(sourceIndex)
                .setDestIndex(destIndex), RequestOptions.DEFAULT);
    }

}
