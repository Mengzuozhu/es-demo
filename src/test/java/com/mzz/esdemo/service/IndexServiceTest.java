package com.mzz.esdemo.service;

import com.mzz.esdemo.common.TestElasticsearchHandler;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.manager.SettingManager;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.ResizeResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zuozhu.meng
 * @since 2020/10/20
 **/
class IndexServiceTest {
    private IndexService indexService = new IndexService(TestElasticsearchHandler.indicesClient());

    @Test
    void createIndex() {
        String index = "temp_index_" + UUID.randomUUID();
        CreateIndexResponse response = indexService.createIndex(index);
        assertTrue(response.isAcknowledged());
        assertTrue(indexService.exists(EsConstant.INDEX_NAME));
        indexService.deleteIndex(index);
    }

    @Test
    void putSettings() {
        Settings settings = SettingManager.getBlocksWriteSettings(false);
        AcknowledgedResponse response = indexService.putSettings(EsConstant.INDEX_NAME, settings);
        assertTrue(response.isAcknowledged());
    }

    @Test
    void cloneIndexWithoutData() {
        String index = "index_clone_without_data";
        indexService.deleteIndex(index);
        CreateIndexResponse response = indexService.cloneIndexWithoutData(EsConstant.INDEX_NAME, index);
        assertTrue(response.isAcknowledged());
    }

    @Test
    void cloneIndex() {
        String indexClone = "index_clone";
        indexService.deleteIndex(indexClone);
        indexService.putSettings(EsConstant.INDEX_NAME, SettingManager.getBlocksWriteSettings(true));
        ResizeResponse response = indexService.cloneIndex(EsConstant.INDEX_NAME, indexClone);
        indexService.putSettings(EsConstant.INDEX_NAME, SettingManager.getBlocksWriteSettings(false));
        assertTrue(response.isAcknowledged());
    }

}
