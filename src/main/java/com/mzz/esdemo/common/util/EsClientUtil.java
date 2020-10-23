package com.mzz.esdemo.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * The type Es client util.
 *
 * @author zuozhu.meng
 * @date 2020 /3/14
 */
public class EsClientUtil {

    /**
     * Create rest high level client rest high level client.
     *
     * @param esUrl the es url
     * @return the rest high level client
     */
    public static RestHighLevelClient createRestHighLevelClient(String esUrl) {
        return new RestHighLevelClient(RestClient.builder(createHttpHost(URI.create(esUrl))));
    }

    /**
     * Create rest high level client rest high level client.
     *
     * @param esIp   the es ip
     * @param esPort the es port
     * @return the rest high level client
     */
    public static RestHighLevelClient createRestHighLevelClient(String esIp, Integer esPort) {
        return new RestHighLevelClient(RestClient.builder(new HttpHost(esIp, esPort, "http")));
    }

    /**
     * Create rest high level client rest high level client.
     *
     * @param esUrl    the es url
     * @param userName the user name
     * @param password the password
     * @return the rest high level client
     */
    public static RestHighLevelClient createRestHighLevelClient(String esUrl, String userName, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        HttpHost httpHost = createHttpHost(URI.create(esUrl));
        return new RestHighLevelClient(RestClient.builder(httpHost)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.disableAuthCaching();
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }));
    }

    /**
     * Create http host http host.
     *
     * @param uri the uri
     * @return the http host
     */
    public static HttpHost createHttpHost(URI uri) {
        if (StringUtils.isEmpty(uri.getUserInfo())) {
            return HttpHost.create(uri.toString());
        }
        try {
            return HttpHost.create(new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment()).toString());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
