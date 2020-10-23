package com.mzz.esdemo.config;

import com.mzz.esdemo.common.util.EsClientUtil;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The type Elasticsearch config.
 *
 * @author zuozhu.meng
 */
@Configuration
public class ElasticsearchConfig {

    @Bean
    RestHighLevelClient restHighLevelClient(@Value("${es.url}") String esUrl) {
        return EsClientUtil.createRestHighLevelClient(esUrl);
    }

    @Bean
    IndicesClient indicesClient(@Value("${es.url}") String esUrl) {
        return EsClientUtil.createRestHighLevelClient(esUrl).indices();
    }
}
