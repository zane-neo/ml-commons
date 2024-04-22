/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.action.models;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.connector.Connector;
import org.opensearch.ml.common.transport.model.MLModelGetAction;
import org.opensearch.ml.common.transport.model.MLModelGetRequest;
import org.opensearch.ml.common.transport.model.MLModelGetResponse;
import org.opensearch.ml.dao.model.ModelDao;
import org.opensearch.ml.helper.ModelAccessControlHelper;
import org.opensearch.ml.utils.RestActionUtils;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.google.common.annotations.VisibleForTesting;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

import java.util.Optional;

@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE)
public class GetModelTransportAction extends HandledTransportAction<ActionRequest, MLModelGetResponse> {

    Client client;
    ClusterService clusterService;

    ModelAccessControlHelper modelAccessControlHelper;

    Settings settings;

    ModelDao modelDao;

    @Inject
    public GetModelTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ClusterService clusterService,
        ModelAccessControlHelper modelAccessControlHelper,
        ModelDao modelDao
    ) {
        super(MLModelGetAction.NAME, transportService, actionFilters, MLModelGetRequest::new);
        this.client = client;
        this.settings = settings;
        this.clusterService = clusterService;
        this.modelAccessControlHelper = modelAccessControlHelper;
        this.modelDao = modelDao;
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<MLModelGetResponse> actionListener) {
        MLModelGetRequest mlModelGetRequest = MLModelGetRequest.fromActionRequest(request);
        String modelId = mlModelGetRequest.getModelId();
        User user = RestActionUtils.getUserContext(client);
        boolean isSuperAdmin = isSuperAdminUserWrapper(clusterService, client);
        Optional<MLModel> modelOptional = modelDao.getModel(modelId, mlModelGetRequest.isReturnContent());
        if (modelOptional.isPresent()) {
            MLModel mlModel = modelOptional.get();
            Boolean isHidden = mlModel.getIsHidden();
            if (isHidden != null && isHidden) {
                if (isSuperAdmin || !mlModelGetRequest.isUserInitiatedGetRequest()) {
                    actionListener.onResponse(MLModelGetResponse.builder().mlModel(mlModel).build());
                } else {
                    actionListener
                        .onFailure(
                            new OpenSearchStatusException(
                                "User doesn't have privilege to perform this operation on this model",
                                RestStatus.FORBIDDEN
                            )
                        );
                }
            } else {
                modelAccessControlHelper
                    .validateModelGroupAccess(user, mlModel.getModelGroupId(), client, ActionListener.wrap(access -> {
                        if (!access) {
                            actionListener
                                .onFailure(
                                    new OpenSearchStatusException(
                                        "User doesn't have privilege to perform this operation on this model",
                                        RestStatus.FORBIDDEN
                                    )
                                );
                        } else {
                            log.debug("Completed Get Model Request, id:{}", modelId);
                            Connector connector = mlModel.getConnector();
                            if (connector != null) {
                                connector.removeCredential();
                            }
                            actionListener.onResponse(MLModelGetResponse.builder().mlModel(mlModel).build());
                        }}, e -> {
                        log.error("Failed to validate Access for Model Id " + modelId, e);
                        actionListener.onFailure(e);
                    }));
            }
        } else {
        actionListener
            .onFailure(
                new OpenSearchStatusException(
                    "Failed to find model with the provided model id: " + modelId,
                    RestStatus.NOT_FOUND
                )
            );
        }
    }

    // this method is only to stub static method.
    @VisibleForTesting
    boolean isSuperAdminUserWrapper(ClusterService clusterService, Client client) {
        return RestActionUtils.isSuperAdminUser(clusterService, client);
    }
}
