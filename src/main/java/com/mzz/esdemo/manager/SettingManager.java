package com.mzz.esdemo.manager;

import org.elasticsearch.common.settings.Settings;

/**
 * The type Setting manager.
 *
 * @author Zero
 */
public class SettingManager {

    private static final String INDEX_BLOCKS_WRITE = "index.blocks.write";

    /**
     * Gets blocks write settings.
     *
     * @param value the value
     * @return the blocks write settings
     */
    public static Settings getBlocksWriteSettings(boolean value) {
        return Settings.builder()
                .put(INDEX_BLOCKS_WRITE, value)
                .build();
    }

}
