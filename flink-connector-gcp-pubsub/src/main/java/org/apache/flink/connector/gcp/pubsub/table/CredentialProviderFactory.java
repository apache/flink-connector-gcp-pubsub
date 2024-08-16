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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;
import static org.apache.flink.connector.gcp.pubsub.table.PubSubOptions.CREDENTIALS_FIXED_ACCESS_EXPIRATION_EPOCH_MILLIS;
import static org.apache.flink.connector.gcp.pubsub.table.PubSubOptions.CREDENTIALS_FIXED_ACCESS_TOKEN;
import static org.apache.flink.connector.gcp.pubsub.table.PubSubOptions.CREDENTIALS_FIXED_JSON;

/**
 * Factory for creating {@link com.google.api.gax.core.CredentialsProvider} from PubSub Table
 * Options.
 */
@Internal
class CredentialProviderFactory {
    private static final String PUBSUB_AUTH_SCOPES = "https://www.googleapis.com/auth/pubsub";
    private static final String PUBSUB_AUTH_CLOUD_PLATFORM_SCOPES =
            "https://www.googleapis.com/auth/cloud-platform";
    private static final String CREDENTIALS_PROVIDER_PREFIX = "credentials-provider.";

    public CredentialsProvider createCredentialsProvider(
            CredentialProviderType credentialType, Map<String, String> options) {
        switch (credentialType) {
            case DEFAULT:
            case GOOGLE_CREDENTIALS:
                return defaultCredentialsProviderBuilder().build();
            case FIXED:
                return FixedCredentialsProvider.create(getFixedCredentialsFromOptions(options));
            default:
                throw new IllegalArgumentException(
                        "Unknown credential provider type: " + credentialType);
        }
    }

    private GoogleCredentials getFixedCredentialsFromOptions(Map<String, String> options) {
        Preconditions.checkArgument(
                options != null
                        && (extractCredentialProviderOption(options, CREDENTIALS_FIXED_JSON) != null
                                || extractCredentialProviderOption(
                                                options, CREDENTIALS_FIXED_ACCESS_TOKEN)
                                        != null),
                "Fixed credential provider requires '"
                        + CREDENTIALS_FIXED_JSON.key()
                        + "' or '"
                        + CREDENTIALS_FIXED_ACCESS_TOKEN.key()
                        + "' options to be set");

        Optional<GoogleCredentials> jsonCredsOptional =
                Optional.ofNullable(
                                extractCredentialProviderOption(options, CREDENTIALS_FIXED_JSON))
                        .map(this::getJsonCredentials);
        Optional<GoogleCredentials> accessTokenCredsOptional =
                Optional.ofNullable(
                                extractCredentialProviderOption(
                                        options, CREDENTIALS_FIXED_ACCESS_TOKEN))
                        .map(
                                accessToken ->
                                        getAccessTokenCredentials(
                                                accessToken,
                                                extractCredentialProviderOption(
                                                        options,
                                                        CREDENTIALS_FIXED_ACCESS_EXPIRATION_EPOCH_MILLIS)));

        if (!jsonCredsOptional.isPresent() && !accessTokenCredsOptional.isPresent()) {
            throw new IllegalArgumentException(
                    "Fixed credential provider requires '"
                            + CREDENTIALS_FIXED_JSON.key()
                            + "' or '"
                            + CREDENTIALS_FIXED_ACCESS_TOKEN.key()
                            + "' options to be set");
        }

        return jsonCredsOptional.orElseGet(accessTokenCredsOptional::get);
    }

    private String extractCredentialProviderOption(
            Map<String, String> options, ConfigOption<?> option) {
        Preconditions.checkNotNull(options);
        Preconditions.checkArgument(option.key().startsWith(CREDENTIALS_PROVIDER_PREFIX));
        return options.get(option.key().substring(CREDENTIALS_PROVIDER_PREFIX.length()));
    }

    private GoogleCredentials getJsonCredentials(String credentialsStringJson) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(credentialsStringJson));

        try {
            return GoogleCredentials.fromStream(
                    new ByteArrayInputStream(credentialsStringJson.getBytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to load Google Credentials from file " + credentialsStringJson, e);
        }
    }

    private GoogleCredentials getAccessTokenCredentials(
            String credentialsAccessToken, String credentialsAccessExpirationEpochMillis) {

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(credentialsAccessToken),
                "Fixed credential provider requires '"
                        + CREDENTIALS_FIXED_JSON.key()
                        + "' or '"
                        + CREDENTIALS_FIXED_ACCESS_TOKEN.key()
                        + "' options to be set");
        Long expirationEpochMillis;

        try {
            expirationEpochMillis =
                    StringUtils.isNullOrWhitespaceOnly(credentialsAccessExpirationEpochMillis)
                            ? null
                            : Long.parseLong(credentialsAccessExpirationEpochMillis);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Failed to parse expiration epoch millis: "
                            + credentialsAccessExpirationEpochMillis,
                    e);
        }

        return GoogleCredentials.create(
                getAccessTokenFromOptions(credentialsAccessToken, expirationEpochMillis));
    }

    private AccessToken getAccessTokenFromOptions(
            String credentialsAccessToken, Long credentialsAccessExpirationEpochMillis) {
        AccessToken.Builder builder =
                AccessToken.newBuilder()
                        .setTokenValue(credentialsAccessToken)
                        .setScopes(
                                Arrays.asList(
                                        PUBSUB_AUTH_CLOUD_PLATFORM_SCOPES, PUBSUB_AUTH_SCOPES));
        if (credentialsAccessExpirationEpochMillis != null) {
            builder.setExpirationTime(
                    Date.from(Instant.ofEpochMilli(credentialsAccessExpirationEpochMillis)));
        }

        return builder.build();
    }
}
