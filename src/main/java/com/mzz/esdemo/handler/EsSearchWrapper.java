package com.mzz.esdemo.handler;

import com.mzz.esdemo.common.util.EsClientUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * The type Es search wrapper.
 *
 * @author zuozhu.meng
 */
@RequiredArgsConstructor
public class EsSearchWrapper {
    private static final RequestOptions DEFAULT_REQUEST = RequestOptions.DEFAULT;
    @Getter
    private final RestHighLevelClient client;

    /**
     * Of es search wrapper.
     *
     * @param client the client
     * @return the es search wrapper
     */
    public static EsSearchWrapper of(RestHighLevelClient client) {
        return new EsSearchWrapper(client);
    }

    /**
     * Of es search wrapper.
     *
     * @param esUrl the es url
     * @return the es search wrapper
     */
    public static EsSearchWrapper of(String esUrl) {
        return of(EsClientUtil.createRestHighLevelClient(esUrl));
    }

    /**
     * Of es search wrapper.
     *
     * @param esIp   the es ip
     * @param esPort the es port
     * @return the es search wrapper
     */
    public static EsSearchWrapper of(String esIp, Integer esPort) {
        return of(EsClientUtil.createRestHighLevelClient(esIp, esPort));
    }

    /**
     * Search or null search response.
     *
     * @param index         the index
     * @param sourceBuilder the source builder
     * @return the search response
     */
    public SearchResponse search(String index, SearchSourceBuilder sourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(index)
                .source(sourceBuilder);
        return search(searchRequest);
    }

    /**
     * Search or null search response.
     *
     * @param request the request
     * @return the search response
     */
    public SearchResponse search(SearchRequest request) {
        try {
            return client.search(request, DEFAULT_REQUEST);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Scroll or null search response.
     *
     * @param request the request
     * @return the search response
     */
    public SearchResponse scroll(SearchScrollRequest request) {
        try {
            return client.scroll(request, DEFAULT_REQUEST);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Bulk.
     *
     * @param bulkRequest the bulk request
     */
    public void bulk(BulkRequest bulkRequest) {
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, DEFAULT_REQUEST);
            if (bulkResponse.hasFailures()) {
                throw new ElasticsearchException(bulkResponse.buildFailureMessage());
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search by page for hit.
     *
     * @param searchRequest the search request
     * @param pageSize      the page size
     * @param consumer      the consumer
     */
    public void searchByPageForHit(SearchRequest searchRequest, int pageSize, Consumer<SearchHit> consumer) {
        try {
            EsFromPageHandler.searchForHit(client, searchRequest, pageSize, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search by page for response.
     *
     * @param searchRequest the search request
     * @param pageSize      the page size
     * @param consumer      the consumer
     */
    public void searchByPageForResponse(SearchRequest searchRequest, int pageSize, Consumer<SearchResponse> consumer) {
        try {
            EsFromPageHandler.searchForResponse(client, searchRequest, pageSize, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search after for hit.
     *
     * @param searchRequest the search request
     * @param consumer      the consumer
     */
    public void searchAfterForHit(SearchRequest searchRequest, Consumer<SearchHit> consumer) {
        try {
            EsSearchAfterHandler.searchForHit(client, searchRequest, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search after for response.
     *
     * @param searchRequest the search request
     * @param consumer      the consumer
     */
    public void searchAfterForResponse(SearchRequest searchRequest, Consumer<SearchResponse> consumer) {
        try {
            EsSearchAfterHandler.searchForResponse(client, searchRequest, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search by scroll for hit.
     *
     * @param searchRequest the search request
     * @param consumer      the consumer
     */
    public void searchByScrollForHit(SearchRequest searchRequest, Consumer<SearchHit> consumer) {
        try {
            new EsScrollHandler(client).searchForHit(searchRequest, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search by scroll for response.
     *
     * @param searchRequest the search request
     * @param consumer      the consumer
     */
    public void searchByScrollForResponse(SearchRequest searchRequest, Consumer<SearchResponse> consumer) {
        try {
            new EsScrollHandler(client).searchForResponse(searchRequest, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Search by scroll for response.
     *
     * @param searchRequest the search request
     * @param keepAlive     the keep alive
     * @param consumer      the consumer
     */
    public void searchByScrollForResponse(SearchRequest searchRequest, TimeValue keepAlive,
                                          Consumer<SearchResponse> consumer) {
        try {
            new EsScrollHandler(client).searchForResponse(searchRequest, keepAlive, consumer);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}
