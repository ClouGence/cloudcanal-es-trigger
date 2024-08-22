package com.clougence.cloudcanal.es7.trigger.ds;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.TransportUtils;

/**
 * @author wanshao create time is 2020/10/27
 **/
public class Es7ClientHelper {

    private static final String  SEMICOLON      = ";";

    private static final String  COLON          = ":";

    private static final String  HTTPS_PROTOCOL = "https";

    private static final Integer HTTPS_PORT     = 443;

    private static final Integer HTTP_PORT      = 80;

    public static RestHighLevelClient generateEsClient(EsConnConfig esConfig) {
        List<HttpHost> hostList = genEsHttpHostList(esConfig.getHosts(), esConfig.isHttpsEnabled());

        RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[] {}));
        builder.setRequestConfigCallback(genRequestConfigCallback(esConfig));
        builder.setHttpClientConfigCallback(genHttpClientConfigCallBack(esConfig.getUserName(), esConfig.getPassword(), esConfig.isCaCertificated(), esConfig
            .getSecurityFileUrl(), esConfig.getMaxConnCount(), esConfig.getMaxConnPerRoute(), esConfig.getKeepAliveSec()));

        return new RestHighLevelClient(builder);
    }

    private static RestClientBuilder.HttpClientConfigCallback genHttpClientConfigCallBack(String userName, String passwd, boolean caCertificated, String securityFileUrl,
                                                                                          Integer maxConnCount, Integer maxConnPerRoute, Integer keepAliveSec) {
        CredentialsProvider cp = null;
        if (StringUtils.isNotEmpty(userName)) {
            cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passwd));
        }

        CredentialsProvider finalCp = cp;

        return builder -> {
            if (maxConnCount != null) {
                builder.setMaxConnTotal(maxConnCount);
            }

            if (maxConnPerRoute != null) {
                builder.setMaxConnPerRoute(maxConnPerRoute);
            }

            if (finalCp != null) {
                builder.setDefaultCredentialsProvider(finalCp);
            }

            if (caCertificated) {
                builder.setSSLContext(initForEs(securityFileUrl));
            }

            if (keepAliveSec != null) {
                builder.setKeepAliveStrategy((response, context) -> TimeUnit.SECONDS.toMillis(keepAliveSec));
            }

            return builder;
        };
    }

    protected static List<HttpHost> genEsHttpHostList(String hostListStr, boolean isHttpsEnabled) {
        String[] hostStrList = hostListStr.split(SEMICOLON);
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String hostStr : hostStrList) {
            String[] hostArray = hostStr.split(COLON);
            String ip = hostArray[0];
            Integer port = null;
            if (hostArray.length > 1) {
                port = Integer.parseInt(hostArray[1]);
            }
            if (isHttpsEnabled) {
                httpHosts.add(new HttpHost(ip, port == null ? HTTPS_PORT : port, HTTPS_PROTOCOL));
            } else {
                httpHosts.add(new HttpHost(ip, port == null ? HTTP_PORT : port));
            }
        }
        return httpHosts;
    }

    private static RestClientBuilder.RequestConfigCallback genRequestConfigCallback(EsConnConfig esConfig) {
        return requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(esConfig.getConnTimeoutMs())
            .setSocketTimeout(esConfig.getSoTimeoutMs())
            .setConnectionRequestTimeout(esConfig.getConnRequestTimeoutMs());
    }

    protected static SSLContext initContext(String trustStoreFileUrl, Function<File, SSLContext> func) {
        File crtFile = new File(trustStoreFileUrl);
        if (!crtFile.exists()) {
            String errorMsg = "CA certificate file NOT EXIST! path:" + crtFile.getAbsolutePath();
            throw new RuntimeException(errorMsg);
        }

        try {
            return func.apply(crtFile);
        } catch (Exception e) {
            String errorMsg = "load CA certificate file Fail! err:" + ExceptionUtils.getRootCauseMessage(e) + ",path:" + crtFile.getAbsolutePath();
            throw new RuntimeException(errorMsg);
        }
    }

    protected static SSLContext initForEs(String securityFileUrl) {
        return initContext(securityFileUrl, f -> {
            try {
                return TransportUtils.sslContextFromHttpCaCrt(f);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
