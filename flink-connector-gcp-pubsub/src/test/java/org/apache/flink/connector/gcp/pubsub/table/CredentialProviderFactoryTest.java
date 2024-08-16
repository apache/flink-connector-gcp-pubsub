/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.gcp.pubsub.table;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Date;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** Tests for {@link CredentialProviderFactory}. */
class CredentialProviderFactoryTest {

    private static final String TEST_CLIENT_ID = "client-id";
    private static final String TEST_JSON_CREDENTIALS;

    static {
        try {
            // Generate a private key for testing
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048); // Key size
            KeyPair keyPair = keyPairGenerator.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PKCS8EncodedKeySpec pkcs8EncodedKeySpec =
                    new PKCS8EncodedKeySpec(privateKey.getEncoded());
            PrivateKey key = KeyFactory.getInstance("RSA").generatePrivate(pkcs8EncodedKeySpec);
            // private key
            String testPkcs8PrivateKey =
                    "-----BEGIN PRIVATE KEY-----\n"
                            + Base64.getEncoder().encodeToString(key.getEncoded()) // private key
                            + "\n-----END PRIVATE KEY-----";
            TEST_JSON_CREDENTIALS =
                    "{\"type\": \"service_account\", "
                            + "\"project_id\": \"project-id\","
                            + " \"private_key_id\": \"1234567890abcdef1234567890abcdef12345678\", "
                            + "\"private_key\": \""
                            + testPkcs8PrivateKey
                            + "\", \"client_email\": \"\", \"client_id\": \""
                            + TEST_CLIENT_ID
                            + "\", "
                            + "\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\","
                            + " \"token_uri\": \"https://oauth2.googleapis.com/token\", "
                            + "\"auth_provider_x509_cert_url\": \"auth-provider-x509-cert-url\", "
                            + "\"client_x509_cert_url\": \"client-x509-cert-url\"}";
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void createDefaultCredentialsProvider() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        CredentialsProvider credentialsProvider =
                factory.createCredentialsProvider(CredentialProviderType.DEFAULT, null);
        assertThat(credentialsProvider).isNotNull();
        assertThat(credentialsProvider).isInstanceOf(GoogleCredentialsProvider.class);
    }

    @Test
    void createGoogleCredentialsProvider() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        CredentialsProvider credentialsProvider =
                factory.createCredentialsProvider(CredentialProviderType.GOOGLE_CREDENTIALS, null);
        assertThat(credentialsProvider).isNotNull();
        assertThat(credentialsProvider).isInstanceOf(GoogleCredentialsProvider.class);
    }

    @Test
    void createFixedCredentialsProviderFailsIfOptionsAreNull() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () -> factory.createCredentialsProvider(CredentialProviderType.FIXED, null))
                .withMessage(
                        "Fixed credential provider requires 'credentials-provider.fixed.credentials-json' or 'credentials-provider.fixed.credentials.access-token' options to be set");
    }

    @Test
    void createFixedCredentialsProviderFailsIfNoOptionsAreProvided() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                factory.createCredentialsProvider(
                                        CredentialProviderType.FIXED, Collections.emptyMap()))
                .withMessage(
                        "Fixed credential provider requires 'credentials-provider.fixed.credentials-json' or 'credentials-provider.fixed.credentials.access-token' options to be set");
    }

    @Test
    void createFixedCredentialsProviderFailsIfAccessTokenIsEmpty() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                factory.createCredentialsProvider(
                                        CredentialProviderType.FIXED,
                                        Collections.singletonMap(
                                                "fixed.credentials.access-token", "")))
                .withMessage(
                        "Fixed credential provider requires 'credentials-provider.fixed.credentials-json' or 'credentials-provider.fixed.credentials.access-token' options to be set");
    }

    @Test
    void createFixedCredentialsProviderPassesIfOnlyAccessTokenIsProvided() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        CredentialsProvider credentialsProvider =
                factory.createCredentialsProvider(
                        CredentialProviderType.FIXED,
                        Collections.singletonMap("fixed.credentials.access-token", "access-token"));
        assertThat(credentialsProvider).isNotNull();
        assertThat(credentialsProvider).isInstanceOf(FixedCredentialsProvider.class);
    }

    @Test
    void createFixedCredentialsProviderFailsIfOnlyExpirationEpochMillisIsProvided() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                factory.createCredentialsProvider(
                                        CredentialProviderType.FIXED,
                                        Collections.singletonMap(
                                                "fixed.credentials.access-expiration-epoch-millis",
                                                "123")))
                .withMessage(
                        "Fixed credential provider requires 'credentials-provider.fixed.credentials-json' or 'credentials-provider.fixed.credentials.access-token' options to be set");
    }

    @Test
    void createFixedCredentialsWithJsonCredentials() throws IOException {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        CredentialsProvider credentialsProvider =
                factory.createCredentialsProvider(
                        CredentialProviderType.FIXED,
                        Collections.singletonMap("fixed.credentials-json", TEST_JSON_CREDENTIALS));

        assertThat(credentialsProvider).isNotNull();
        assertThat(credentialsProvider).isInstanceOf(FixedCredentialsProvider.class);

        Credentials credentials = ((FixedCredentialsProvider) credentialsProvider).getCredentials();
        assertThat(credentials).isNotNull();
        assertThat(credentials)
                .isEqualTo(
                        GoogleCredentials.fromStream(
                                new java.io.ByteArrayInputStream(
                                        TEST_JSON_CREDENTIALS.getBytes())));
    }

    @Test
    void createFixedCredentialsWithAccessToken() {
        CredentialProviderFactory factory = new CredentialProviderFactory();
        Map<String, String> options = new HashMap<>();
        options.put("fixed.credentials.access-token", "access-token");
        options.put("fixed.credentials.access-expiration-epoch-millis", "123");
        CredentialsProvider credentialsProvider =
                factory.createCredentialsProvider(CredentialProviderType.FIXED, options);

        assertThat(credentialsProvider).isNotNull();
        assertThat(credentialsProvider).isInstanceOf(FixedCredentialsProvider.class);

        Credentials credentials = ((FixedCredentialsProvider) credentialsProvider).getCredentials();
        assertThat(credentials).isNotNull();
        assertThat(credentials)
                .isEqualTo(
                        GoogleCredentials.create(
                                new AccessToken(
                                        "access-token",
                                        Date.from(java.time.Instant.ofEpochMilli(123)))));
    }
}
