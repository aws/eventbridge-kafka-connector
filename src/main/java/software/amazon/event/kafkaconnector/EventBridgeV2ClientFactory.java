/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;

import java.net.URI;
import org.apache.kafka.connect.errors.ConnectException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridgev2.EventBridgeV2AsyncClient;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.event.kafkaconnector.auth.EventBridgeAwsCredentialsProviderFactory;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;

/** Creates the Custom Event Bus (eventsv2) client mirroring the v1 writer's client construction. */
class EventBridgeV2ClientFactory {

  private EventBridgeV2ClientFactory() {}

  static EventBridgeV2AsyncClient create(EventBridgeSinkConfig config) {
    var endpointUri =
        StringUtils.trim(config.endpointURI).isBlank() ? null : URI.create(config.endpointURI);

    var retryPolicy =
        RetryPolicy.forRetryMode(RetryMode.STANDARD).toBuilder()
            .numRetries(config.maxRetries)
            .build();

    var userAgentPrefix =
        String.format(
            "%s/%s", PropertiesUtil.getConnectorName(), PropertiesUtil.getConnectorVersion());

    var clientConfig =
        ClientOverrideConfiguration.builder()
            .retryPolicy(retryPolicy)
            .putAdvancedOption(USER_AGENT_PREFIX, userAgentPrefix)
            .build();

    var credentialsProvider =
        EventBridgeAwsCredentialsProviderFactory.getAwsCredentialsProvider(config);

    var client =
        EventBridgeV2AsyncClient.builder()
            .region(Region.of(config.region))
            .endpointOverride(endpointUri)
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder())
            .overrideConfiguration(clientConfig)
            .credentialsProvider(credentialsProvider)
            .build();

    // fail fast if credentials cannot be resolved
    try {
      credentialsProvider.resolveCredentials();
    } catch (Exception e) {
      client.close();
      throw new ConnectException(e);
    }
    return client;
  }
}
