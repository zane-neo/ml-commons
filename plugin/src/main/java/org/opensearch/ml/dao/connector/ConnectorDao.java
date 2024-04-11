package org.opensearch.ml.dao.connector;

import org.opensearch.ml.common.connector.Connector;

import java.util.Optional;

public interface ConnectorDao {

    String createConnector(Connector connector) throws Exception ;

    Optional<Connector> getConnector(String connectorId, boolean isReturnContent) throws Exception;
}
