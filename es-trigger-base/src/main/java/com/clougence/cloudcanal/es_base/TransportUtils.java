package com.clougence.cloudcanal.es_base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class TransportUtils {

    /**
     * Creates an <code>SSLContext</code> from the self-signed <code>http_ca.crt</code> certificate created by Elasticsearch during
     * its first start.
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-stack-security.html">Elasticsearch
     *      documentation</a>
     */
    public static SSLContext sslContextFromHttpCaCrt(File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            return sslContextFromHttpCaCrt(in);
        }
    }

    /**
     * Creates an <code>SSLContext</code> from the self-signed <code>http_ca.crt</code> certificate created by Elasticsearch during
     * its first start.
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-stack-security.html">Elasticsearch
     *      documentation</a>
     */
    public static SSLContext sslContextFromHttpCaCrt(InputStream in) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate certificate = cf.generateCertificate(in);

            final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setCertificateEntry("elasticsearch-ca", certificate);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;

        } catch (CertificateException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates an <code>SSLContext</code> from the SHA-256 fingerprint of self-signed <code>http_ca.crt</code> certificate output by
     * Elasticsearch at startup time.
     *
     * @param fingerPrint the SHA-256 fingerprint. Can be uppercase or lowercase, with or without colons separating bytes
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-stack-security.html">Elasticsearch
     *      documentation</a>
     */
    public static SSLContext sslContextFromCaFingerprint(String fingerPrint) {

        fingerPrint = fingerPrint.replace(":", "");
        int len = fingerPrint.length();
        byte[] fpBytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            fpBytes[i / 2] = (byte) ((Character.digit(fingerPrint.charAt(i), 16) << 4) + Character.digit(fingerPrint.charAt(i + 1), 16));
        }

        try {
            X509TrustManager tm = new X509TrustManager() {

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    throw new CertificateException("This is a client-side only trust manager");
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

                    // The CA root is the last element of the chain
                    X509Certificate anchor = chain[chain.length - 1];

                    byte[] bytes;
                    try {
                        MessageDigest md = MessageDigest.getInstance("SHA-256");
                        md.update(anchor.getEncoded());
                        bytes = md.digest();
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }

                    if (Arrays.equals(fpBytes, bytes)) {
                        return;
                    }

                    throw new CertificateException("Untrusted certificate: " + anchor.getSubjectX500Principal());
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new X509TrustManager[] { tm }, null);
            return sslContext;

        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            // Exceptions that should normally not occur
            throw new RuntimeException(e);
        }
    }
}
