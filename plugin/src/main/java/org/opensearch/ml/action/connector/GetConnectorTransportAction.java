/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.action.connector;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.connector.Connector;
import org.opensearch.ml.common.transport.connector.MLConnectorGetAction;
import org.opensearch.ml.common.transport.connector.MLConnectorGetRequest;
import org.opensearch.ml.common.transport.connector.MLConnectorGetResponse;
import org.opensearch.ml.dao.connector.ConnectorDao;
import org.opensearch.ml.helper.ConnectorAccessControlHelper;
import org.opensearch.ml.utils.RestActionUtils;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Optional;

@Log4j2
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class GetConnectorTransportAction extends HandledTransportAction<ActionRequest, MLConnectorGetResponse> {

    Client client;
    NamedXContentRegistry xContentRegistry;

    ConnectorAccessControlHelper connectorAccessControlHelper;

    ConnectorDao connectorDao;

    @Inject
    public GetConnectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ConnectorAccessControlHelper connectorAccessControlHelper,
        ConnectorDao connectorDao
    ) {
        super(MLConnectorGetAction.NAME, transportService, actionFilters, MLConnectorGetRequest::new);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.connectorAccessControlHelper = connectorAccessControlHelper;
        this.connectorDao = connectorDao;
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<MLConnectorGetResponse> actionListener) {
        MLConnectorGetRequest mlConnectorGetRequest = MLConnectorGetRequest.fromActionRequest(request);
        String connectorId = mlConnectorGetRequest.getConnectorId();
        User user = RestActionUtils.getUserContext(client);
        try {
            Optional<Connector> optionalConnector = connectorDao.getConnector(connectorId,
                    mlConnectorGetRequest.isReturnContent());
            if (optionalConnector.isPresent()) {
                Connector mlConnector = optionalConnector.get();
                // TODO: Pass tenant ID as part of get connector operation to perforc check as part of DAO layer.
                if (!mlConnector.getTenantId().equals(mlConnectorGetRequest.getTenantId())) {
                    actionListener
                            .onFailure(
                                    new OpenSearchStatusException(
                                            "You don't have permission to access this connector",
                                            RestStatus.FORBIDDEN
                                    )
                            );
                }
                mlConnector.removeCredential();
                if (connectorAccessControlHelper.hasPermission(user, mlConnector)) {
                    actionListener.onResponse(MLConnectorGetResponse.builder().mlConnector(mlConnector).build());
                } else {
                    actionListener
                        .onFailure(
                            new OpenSearchStatusException(
                                "You don't have permission to access this connector",
                                RestStatus.FORBIDDEN
                            )
                        );
                }
            } else {
                actionListener
                    .onFailure(
                        new OpenSearchStatusException(
                            "Failed to find connector with the provided connector id: " + connectorId,
                            RestStatus.NOT_FOUND
                        )
                    );
            }
        } catch (Exception e) {
            log.error("Failed to get ML connector " + connectorId, e);
            actionListener.onFailure(e);
        }

    }
}
