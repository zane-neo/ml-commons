package org.opensearch.ml.dao.model;

import lombok.extern.log4j.Log4j2;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.ml.common.MLModel;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;

@Log4j2
public class OpenSearchRestModelDao implements ModelDao {

    private static final String ML_MODEL_INDEX = "oasis_ml_model";

    private OpenSearchClient openSearchClient;

    public OpenSearchRestModelDao(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }
    @Override
    public String createModel(MLModel mlModel) {
        try {
            IndexRequest<MLModel> indexRequest = new IndexRequest.Builder<MLModel>().index(ML_MODEL_INDEX)
                    .document(mlModel).build();
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
    public Optional<MLModel> getModel(String modelId, boolean isReturnContent) {
        GetResponse<MLModel> getResponse = AccessController.doPrivileged((PrivilegedAction<GetResponse<MLModel>>) () -> {
            try {
                return openSearchClient.get(getRequest -> getRequest.index(ML_MODEL_INDEX).id(modelId), MLModel.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return getResponse.source() == null ? Optional.empty() : Optional.of(getResponse.source());
    }
}
