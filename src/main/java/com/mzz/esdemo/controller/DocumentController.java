package com.mzz.esdemo.controller;

import com.alibaba.fastjson.JSONObject;
import com.mzz.esdemo.model.Response;
import com.mzz.esdemo.service.DocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * The type Document controller.
 *
 * @author zuozhu.meng
 * @since 2020 /10/20
 */
@RestController
@RequestMapping(value = "/doc")
@RequiredArgsConstructor
public class DocumentController {

    private final DocumentService documentService;

    /**
     * Upsert.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the response
     */
    @PostMapping("/{index}/{id}/upsert")
    public Response<?> upsert(@PathVariable String index, @PathVariable String id, @RequestBody JSONObject source) {
        return Response.of(documentService.upsertDoc(index, id, source));
    }

    /**
     * Upsert.
     *
     * @param index   the index
     * @param sources the sources
     * @return the response
     */
    @PostMapping("/{index}/multiUpsert")
    public Response<?> upsert(@PathVariable String index, @RequestBody List<JSONObject> sources) {
        return Response.of(documentService.upsertDocByBulk(index, sources));
    }

    /**
     * Gets doc.
     *
     * @param index the index
     * @param id    the id
     * @return the doc
     */
    @GetMapping("/{index}/{id}")
    public Response<?> getDoc(@PathVariable String index, @PathVariable String id) {
        return Response.of(documentService.getDoc(index, id).getSourceAsMap());
    }

    /**
     * Delete.
     *
     * @param index the index
     * @param id    the id
     * @return the response
     */
    @DeleteMapping("/{index}/{id}")
    public Response<?> delete(@PathVariable String index, @PathVariable String id) {
        return Response.of(documentService.deleteDoc(index, id));
    }

    /**
     * Delete.
     *
     * @param index  the index
     * @param source the source
     * @return the response
     */
    @PostMapping("/{index}/deleteByQuery")
    public Response<?> delete(@PathVariable String index, @RequestBody JSONObject source) {
        return Response.of(documentService.deleteByQuery(index, source.toJSONString()));
    }

    /**
     * Clear index.
     *
     * @param index the index
     * @return the response
     */
    @PostMapping("/{index}/clearIndex")
    public Response<?> clearIndex(@PathVariable String index) {
        return Response.of(documentService.clearIndex(index));
    }

    /**
     * Reindex.
     *
     * @param sourceIndex the source index
     * @param destIndex   the dest index
     * @return the response
     */
    @PostMapping("/{sourceIndex}/{destIndex}/reindex")
    public Response<?> reindex(@PathVariable String sourceIndex, @PathVariable String destIndex) {
        return Response.of(documentService.reindex(sourceIndex, destIndex));
    }
}
