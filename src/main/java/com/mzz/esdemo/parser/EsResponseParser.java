package com.mzz.esdemo.parser;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The type Es response parser.
 *
 * @author zuozhu.meng
 */
public class EsResponseParser {

    /**
     * For each hits.
     *
     * @param searchResponse the search response
     * @param consumer       the consumer
     */
    public static void forEachHits(SearchResponse searchResponse, Consumer<SearchHit> consumer) {
        if (searchResponse == null) {
            return;
        }
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        for (SearchHit searchHit : searchHits) {
            consumer.accept(searchHit);
        }
    }

    /**
     * For each hits as map.
     *
     * @param searchResponse the search response
     * @param consumer       the consumer
     */
    public static void forEachHitsAsMap(SearchResponse searchResponse, Consumer<Map<String, Object>> consumer) {
        forEachHits(searchResponse, hit -> consumer.accept(hit.getSourceAsMap()));
    }

    /**
     * Response hits to list list.
     *
     * @param searchResponse the search response
     * @return the list
     */
    public static List<Map<String, Object>> responseHitsToList(SearchResponse searchResponse) {
        List<Map<String, Object>> res = new ArrayList<>((int) searchResponse.getHits().getTotalHits().value);
        EsResponseParser.forEachHitsAsMap(searchResponse, res::add);
        return res;
    }
}
