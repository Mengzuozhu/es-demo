package com.mzz.esdemo.handler;

import com.mzz.esdemo.parser.EsResponseParser;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * ES Scroll查询
 *
 * @author zuozhu.meng
 **/
@RequiredArgsConstructor
public class EsScrollHandler {
    private static final int DEFAULT_SCROLL_TIME_MILLIS = 60000;
    private final RestHighLevelClient highLevelClient;
    @Setter
    private boolean isClearScroll = true;

    public EsScrollHandler(RestHighLevelClient client, boolean isClearScroll) {
        this.isClearScroll = isClearScroll;
        this.highLevelClient = client;
    }

    /**
     * Search for hit.
     *
     * @param searchRequest the request builder
     * @param consumer      the consumer
     */
    public void searchForHit(SearchRequest searchRequest, Consumer<SearchHit> consumer) throws IOException {
        searchForResponse(searchRequest, searchResponse -> EsResponseParser.forEachHits(searchResponse, consumer));
    }

    /**
     * Search for response.
     *
     * @param searchRequest the request builder
     * @param consumer      the consumer
     */
    public void searchForResponse(SearchRequest searchRequest, Consumer<SearchResponse> consumer) throws IOException {
        searchForResponse(searchRequest, new TimeValue(DEFAULT_SCROLL_TIME_MILLIS), consumer);
    }

    /**
     * Search for response.
     *
     * @param searchRequest the request builder
     * @param consumer      the consumer
     * @param keepAlive     the keep alive
     */
    public void searchForResponse(SearchRequest searchRequest, TimeValue keepAlive,
                                  Consumer<SearchResponse> consumer) throws IOException {
        if (searchRequest == null || consumer == null) {
            return;
        }
        searchRequest.scroll(keepAlive);
        SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        long length = searchResponse.getHits().getHits().length;
        String scrollId = null;
        while (length > 0) {
            consumer.accept(searchResponse);
            scrollId = searchResponse.getScrollId();
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId)
                    .scroll(keepAlive);
            searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            length = searchResponse.getHits().getHits().length;
        }
        // 清空快照记录，避免内存占用
        if (isClearScroll && scrollId != null) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            clearScrollAsync(clearScrollRequest);
        }
    }

    private void clearScrollAsync(ClearScrollRequest request) {
        highLevelClient.clearScrollAsync(request, RequestOptions.DEFAULT, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse clearScrollResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                throw new ElasticsearchException(e);
            }
        });
    }

}
