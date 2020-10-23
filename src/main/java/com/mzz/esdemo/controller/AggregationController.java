package com.mzz.esdemo.controller;

import com.mzz.esdemo.model.Response;
import com.mzz.esdemo.service.AggregationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * The type Aggregation controller.
 *
 * @author zuozhu.meng
 * @since 2020 /10/21
 */
@RestController
@RequiredArgsConstructor
public class AggregationController {
    /**
     * The Aggregation service.
     */
    private final AggregationService aggregationService;

    /**
     * Distinct count response.
     *
     * @param index the index
     * @param field the field
     * @return the response
     */
    @GetMapping("/{index}/{field}/distinctCount")
    public Response distinctCount(@PathVariable String index, @PathVariable String field) {
        return Response.of(aggregationService.distinctCount(index, field));
    }

    /**
     * Min agg response.
     *
     * @param index the index
     * @param field the field
     * @return the response
     */
    @GetMapping("/{index}/{field}/min")
    public Response minAgg(@PathVariable String index, @PathVariable String field) {
        return Response.of(aggregationService.minAgg(index, field));
    }

    /**
     * Max agg response.
     *
     * @param index the index
     * @param field the field
     * @return the response
     */
    @GetMapping("/{index}/{field}/max")
    public Response maxAgg(@PathVariable String index, @PathVariable String field) {
        return Response.of(aggregationService.maxAgg(index, field));
    }

    /**
     * Avg agg response.
     *
     * @param index the index
     * @param field the field
     * @return the response
     */
    @GetMapping("/{index}/{field}/avg")
    public Response avgAgg(@PathVariable String index, @PathVariable String field) {
        return Response.of(aggregationService.avgAgg(index, field));
    }

    /**
     * Terms count agg response.
     *
     * @param index the index
     * @param field the field
     * @return the response
     */
    @GetMapping("/{index}/{field}/termsCount")
    public Response termsCountAgg(@PathVariable String index, @PathVariable String field) {
        return Response.of(aggregationService.termsCountAgg(index, field));
    }
}
