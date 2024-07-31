package com.clougence.cloudcanal.es_base;

import lombok.Getter;
import lombok.Setter;

/**
 * @author bucketli 2024/7/30 14:58:30
 */
@Getter
@Setter
public class EsConnConfig {

    private String  hosts;

    private String  userName;

    private String  password;

    private boolean httpsEnabled;

    private boolean caCertificated;

    private Integer soTimeoutMs          = 120 * 1000;

    private Integer connTimeoutMs        = 30 * 1000;

    private Integer connRequestTimeoutMs = 60 * 1000;

    private Integer maxConnCount         = 32;

    private Integer maxConnPerRoute      = 8;

    private Integer keepAliveSec         = 60;

    private String  securityFileUrl;
}
