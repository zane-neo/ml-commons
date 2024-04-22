package org.opensearch.ml.module;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Provides;
import org.opensearch.ml.dao.connector.ConnectorDao;
import org.opensearch.ml.dao.connector.OpenSearchRestConnectorDao;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.util.Optional;

@RequiredArgsConstructor
public class MetaDataAccessModule extends AbstractModule {
    public static final String REMOTE_METADATA_ENDPOINT = "REMOTE_METADATA_ENDPOINT";
    public static final String REGION = "REGION";

    @Override
    protected void configure() {}

    @Provides
    public ConnectorDao createConnectorDao() {
        return new OpenSearchRestConnectorDao(createOpenSearchClient());
    }

    public OpenSearchClient createOpenSearchClient() {
        SdkHttpClient httpClient = ApacheHttpClient.builder().build();
        try {
            return new OpenSearchClient(
                    new AwsSdk2Transport(
                            httpClient,
                            Optional.ofNullable(System.getenv(REMOTE_METADATA_ENDPOINT)).orElse("http://localhost:9200"),
                            Region.of(Optional.ofNullable(System.getenv(REGION)).orElse("us-east-1")),
                            AwsSdk2TransportOptions.builder().build()
                    )
            );
        } catch (Exception e) {
            throw e;
        }
    }
}
