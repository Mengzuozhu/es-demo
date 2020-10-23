package com.mzz.esdemo.service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;

/**
 * The type Search service.
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high-query-builders.html">
 *
 * @author zuozhu.meng
 * @since 2020 /10/20
 */
@Service
@RequiredArgsConstructor
public class SearchService {
    /**
     * The Rest high level client.
     */
    private final RestHighLevelClient restHighLevelClient;

    /**
     * Match all query search response.
     *
     * @param index the index
     * @return the search response
     */
    public SearchResponse matchAllQuery(String index) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        return search(index, sourceBuilder);
    }

    /**
     * Terms query search response.
     *
     * @param index  the index
     * @param field  the field
     * @param values the values
     * @return the search response
     */
    public SearchResponse termsQuery(String index, String field, Collection<?> values) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.termsQuery(field, values));
        return search(index, sourceBuilder);
    }

    /**
     * Range query search response.
     *
     * @param index the index
     * @param field the field
     * @param gte   the gte
     * @param lte   the lte
     * @return the search response
     */
    public SearchResponse rangeQuery(String index, String field, Object gte, Object lte) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery(field)
                        .gte(gte)
                        .lte(lte));
        return search(index, sourceBuilder);
    }

    /**
     * Match query search response.
     *
     * @param index the index
     * @param field the field
     * @param text  the text
     * @return the search response
     */
    public SearchResponse matchQuery(String index, String field, Object text) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(field, text));
        return search(index, sourceBuilder);
    }

    /**
     * Match phrase query search response.
     *
     * @param index the index
     * @param field the field
     * @param text  the text
     * @return the search response
     */
    public SearchResponse matchPhraseQuery(String index, String field, Object text) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchPhraseQuery(field, text));
        return search(index, sourceBuilder);
    }

    /**
     * Query string query search response.
     *
     * @param index       the index
     * @param queryString the query string
     * @return the search response
     */
    public SearchResponse queryStringQuery(String index, String queryString) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery(queryString));
        return search(index, sourceBuilder);
    }

    /**
     * Query by json search response.
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-search.html">
     *
     * @param index       the index
     * @param contentJson the query json
     * @return the search response
     */
    @SneakyThrows
    public SearchResponse queryByJson(String index, String contentJson) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(getContentRegistry(), LoggingDeprecationHandler.INSTANCE, contentJson)) {
            sourceBuilder.parseXContent(parser);
        }
        return search(index, sourceBuilder);
    }

    private NamedXContentRegistry getContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    /**
     * Search search response.
     *
     * @param index         the index
     * @param sourceBuilder the source builder
     * @return the search response
     */
    @SneakyThrows
    private SearchResponse search(String index, SearchSourceBuilder sourceBuilder) {
        SearchRequest searchRequest = new SearchRequest(index)
                .source(sourceBuilder);
        return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

}
