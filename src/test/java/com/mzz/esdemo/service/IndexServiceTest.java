package com.mzz.esdemo.service;

import com.mzz.esdemo.EsDemoApplication;
import com.mzz.esdemo.common.constant.EsConstant;
import com.mzz.esdemo.manager.SettingManager;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.ResizeResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author zuozhu.meng
 * @since 2020/10/20
 **/
@SpringBootTest(classes = EsDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IndexServiceTest {
    @Autowired
    private IndexService indexService;

    @Test
    void putSettings() {
        Settings settings = SettingManager.getBlocksWriteSettings(false);
        AcknowledgedResponse response = indexService.putSettings(EsConstant.INDEX_NAME, settings);
        Assertions.assertTrue(response.isAcknowledged());
    }

    @Test
    void cloneIndexWithoutData() {
        String index = "index_clone_without_data";
        indexService.deleteIndex(index);
        CreateIndexResponse response = indexService.cloneIndexWithoutData(EsConstant.INDEX_NAME, index);
        Assertions.assertTrue(response.isAcknowledged());
    }

    @Test
    void cloneIndex() {
        String indexClone = "index_clone";
        indexService.deleteIndex(indexClone);
        indexService.putSettings(EsConstant.INDEX_NAME, SettingManager.getBlocksWriteSettings(true));
        ResizeResponse response = indexService.cloneIndex(EsConstant.INDEX_NAME, indexClone);
        indexService.putSettings(EsConstant.INDEX_NAME, SettingManager.getBlocksWriteSettings(false));
        Assertions.assertTrue(response.isAcknowledged());
    }

    @Test
    void exists() {
        Assertions.assertTrue(indexService.exists(EsConstant.INDEX_NAME));
    }
}
