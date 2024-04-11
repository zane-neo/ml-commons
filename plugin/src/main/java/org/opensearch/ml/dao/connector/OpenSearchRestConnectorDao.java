package org.opensearch.ml.dao.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.ml.common.connector.Connector;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.stream.Collectors;

@Log4j2
public class OpenSearchRestConnectorDao implements ConnectorDao {

    private static final String ML_CONNECTOR_INDEX = "oasis_ml_connector";

    private OpenSearchClient openSearchClient;

    public OpenSearchRestConnectorDao(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }
    @Override
    public String createConnector(Connector connector) throws Exception {
        try {
            IndexRequest<Connector> indexRequest = new IndexRequest.Builder<Connector>().index(ML_CONNECTOR_INDEX)
                    .document(connector).build();
            final IndexResponse indexResponse =  AccessController.doPrivileged((PrivilegedAction<IndexResponse>) () -> {
                try {
                    return openSearchClient.index(indexRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return indexResponse.id();
        } catch (Exception e) {
            log.error("Exception : " + e);
            throw e;
        }
    }

    @Override
    public Optional<Connector> getConnector(String connectorId, boolean isReturnContent) throws Exception {
        GetResponse<Connector> getResponse = AccessController.doPrivileged((PrivilegedAction<GetResponse>) () -> {
            try {
                return openSearchClient.get(getRequest -> getRequest.index(ML_CONNECTOR_INDEX).id(connectorId), Connector.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return getResponse.source() == null ? Optional.empty() : Optional.of(getResponse.source());
    }
}
