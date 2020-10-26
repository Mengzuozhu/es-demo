package com.mzz.esdemo.handler;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.function.Consumer;

import static com.mzz.esdemo.parser.EsResponseParser.forEachHits;

/**
 * ES from size分页查询
 *
 * @author zuozhu.meng
 **/
public class EsFromPageHandler {

    /**
     * Search for hit.
     *
     * @param client        the client
     * @param searchRequest the search request
     * @param pageSize      the page size
     * @param consumer      the consumer
     * @throws IOException the io exception
     */
    public static void searchForHit(RestHighLevelClient client, SearchRequest searchRequest, int pageSize,
                                    Consumer<SearchHit> consumer) throws IOException {
        searchForResponse(client, searchRequest, pageSize, searchResponse -> forEachHits(searchResponse, consumer));
    }

    /**
     * Search for response.
     *
     * @param client        the client
     * @param searchRequest the search request
     * @param pageSize      the page size
     * @param consumer      the consumer
     * @throws IOException the io exception
     */
    public static void searchForResponse(RestHighLevelClient client, SearchRequest searchRequest, int pageSize,
                                         Consumer<SearchResponse> consumer) throws IOException {
        if (searchRequest == null || consumer == null) {
            return;
        }

        searchRequest.source()
                .trackTotalHits(true)
                .size(0);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        long totalHits = searchResponse.getHits().getTotalHits().value;
        int from = 0;
        while (from < totalHits) {
            searchRequest.source()
                    .size(pageSize)
                    .from(from);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            consumer.accept(searchResponse);
            from += pageSize;
        }
    }

}
