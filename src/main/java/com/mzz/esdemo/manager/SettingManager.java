package com.mzz.esdemo.manager;

import org.elasticsearch.common.settings.Settings;

/**
 * @author zuozhu.meng
 * @since 2020/10/21
 **/
public class SettingManager {

    private static final String INDEX_BLOCKS_WRITE = "index.blocks.write";

    public static Settings getBlocksWriteSettings(boolean value) {
        return Settings.builder()
                .put(INDEX_BLOCKS_WRITE, value)
                .build();
    }

}
