package com.mzz.esdemo.controller;

import com.mzz.esdemo.model.Response;
import com.mzz.esdemo.service.IndexService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The type Index controller.
 *
 * @author zuozhu.meng
 * @since 2020 /10/22
 */
@RestController
@RequiredArgsConstructor
public class IndexController {
    private final IndexService indexService;

    /**
     * Gets single mappings.
     *
     * @param index the index
     * @return the single mappings
     */
    @GetMapping("/{index}/mapping")
    public Response<?> getSingleMappings(@PathVariable String index) {
        return Response.of(indexService.getSingleMappings(index));
    }

    /**
     * Clone index without data.
     *
     * @param sourceIndex the source index
     * @param targetIndex the target index
     * @return the response
     */
    @PostMapping("/{sourceIndex}/{targetIndex}/cloneIndexWithoutData")
    public Response<?> cloneIndexWithoutData(@PathVariable String sourceIndex, @PathVariable String targetIndex) {
        return Response.of(indexService.cloneIndexWithoutData(sourceIndex, targetIndex));
    }

}
