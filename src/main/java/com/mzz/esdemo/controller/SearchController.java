package com.mzz.esdemo.controller;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.model.Response;
import com.mzz.esdemo.parser.EsResponseParser;
import com.mzz.esdemo.service.SearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * The type Search controller.
 *
 * @author zuozhu.meng
 * @since 2020 /10/21
 */
@RestController
@RequiredArgsConstructor
public class SearchController {
    private final SearchService searchService;

    /**
     * Match all query.
     *
     * @param index the index
     * @return the response
     */
    @GetMapping("/{index}/matchAllQuery")
    public Response<?> matchAllQuery(@PathVariable String index) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.matchAllQuery(index)));
    }

    /**
     * Terms query.
     *
     * @param index  the index
     * @param field  the field
     * @param values the values
     * @return the response
     */
    @PostMapping("/{index}/{field}/termsQuery")
    public Response<?> termsQuery(@PathVariable String index, @PathVariable String field,
                                  @RequestBody List<Object> values) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.termsQuery(index, field, values)));
    }

    /**
     * Terms query.
     *
     * @param index the index
     * @param field the field
     * @param gte   the gte
     * @param lte   the lte
     * @return the response
     */
    @GetMapping("/{index}/{field}/rangeQuery")
    public Response<?> rangeQuery(@PathVariable String index, @PathVariable String field,
                                  @RequestParam Object gte, @RequestParam Object lte) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.rangeQuery(index, field, gte, lte)));
    }

    /**
     * Match query.
     *
     * @param index the index
     * @param field the field
     * @param text  the text
     * @return the response
     */
    @GetMapping("/{index}/{field}/matchQuery")
    public Response<?> matchQuery(@PathVariable String index, @PathVariable String field, @RequestParam Object text) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.matchQuery(index, field, text)));
    }

    /**
     * Match phrase query.
     *
     * @param index the index
     * @param field the field
     * @param text  the text
     * @return the response
     */
    @GetMapping("/{index}/{field}/matchPhraseQuery")
    public Response<?> matchPhraseQuery(@PathVariable String index, @PathVariable String field,
                                        @RequestParam Object text) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.matchPhraseQuery(index, field,
                text)));
    }

    /**
     * Query string query.
     *
     * @param index the index
     * @param text  the text
     * @return the response
     */
    @GetMapping("/{index}/queryStringQuery")
    public Response<?> queryStringQuery(@PathVariable String index, @RequestParam String text) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.queryStringQuery(index, text)));
    }

    /**
     * Query by json.
     *
     * @param index   the index
     * @param content the content
     * @return the response
     */
    @PostMapping("/{index}/queryByJson")
    public Response<?> queryByJson(@PathVariable String index, @RequestBody JSONObject content) {
        return Response.of(EsResponseParser.responseHitsToList(searchService.queryByJson(index,
                content.toJSONString())));
    }

}
