package org.opensearch.ml.dao.model;

import lombok.extern.log4j.Log4j2;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.engine.indices.MLIndicesHandler;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.util.Optional;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.ml.common.CommonValue.ML_MODEL_INDEX;
import static org.opensearch.ml.common.MLModel.ALGORITHM_FIELD;
import static org.opensearch.ml.utils.MLNodeUtils.createXContentParserFromRegistry;
import static org.opensearch.ml.utils.RestActionUtils.getFetchSourceContext;

@Log4j2
public class OpenSearchTransportModelDao implements ModelDao {

    private Client client;
    private MLIndicesHandler mlIndicesHandler;

    private NamedXContentRegistry xContentRegistry;

    public OpenSearchTransportModelDao(Client client,
                                           MLIndicesHandler mlIndicesHandler,
                                           NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.mlIndicesHandler = mlIndicesHandler;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    public String createModel(MLModel mlModel) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            mlIndicesHandler.initModelIndexIfAbsent();
            IndexRequest indexRequest = new IndexRequest(ML_MODEL_INDEX);
            indexRequest.source(mlModel.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS));
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            final IndexResponse indexResponse = client.index(indexRequest).actionGet();
            context.restore();
            return indexResponse.getId();
        } catch (Exception e) {
            log.error("Failed to create model!", e);
            return null;
        }
    }

    @Override
    public Optional<MLModel> getModel(String modelId, boolean isReturnContent) {
        FetchSourceContext fetchSourceContext = getFetchSourceContext(isReturnContent);
        GetRequest getRequest = new GetRequest(ML_MODEL_INDEX).id(modelId).fetchSourceContext(fetchSourceContext);

        ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext();
        try {
            GetResponse r = client.get(getRequest).actionGet();
            log.debug("Completed Get Model Request, id:{}", modelId);

            if (r != null && r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    String algorithmName = r.getSource().get(ALGORITHM_FIELD).toString();
                    MLModel mlModel = MLModel.parse(parser, algorithmName);
                    return Optional.of(mlModel);
                } catch (Exception e) {
                    log.error("Failed to parse ml model" + r.getId(), e);
                    throw e;
                }
            }
            return Optional.empty();
        } catch(Exception e) {
            if (e instanceof IndexNotFoundException) {
                log.error("Failed to get model index", e);
                throw new OpenSearchStatusException("Failed to find model", RestStatus.NOT_FOUND);
            } else {
                log.error("Failed to get ML model " + modelId, e);
                throw new IllegalStateException("Failed to get ML model " + modelId, e);
            }
        } finally {
            context.restore();
        }
    }
}
