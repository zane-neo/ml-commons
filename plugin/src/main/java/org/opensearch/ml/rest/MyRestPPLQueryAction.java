/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.OK;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.plugin.request.PPLQueryRequestFactory;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

public class MyRestPPLQueryAction extends BaseRestHandler {
    public static final String QUERY_API_ENDPOINT = "_ml/_ppl";
    public static final String EXPLAIN_API_ENDPOINT = "_ml/_ppl/_explain";
    public static final String LEGACY_QUERY_API_ENDPOINT = "_ml/_opendistro/_ppl";
    public static final String LEGACY_EXPLAIN_API_ENDPOINT = "_ml/_opendistro/_ppl/_explain";

    private static final Logger LOG = LogManager.getLogger();

    /** Constructor of RestPPLQueryAction. */
    public MyRestPPLQueryAction() {
        super();
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, QUERY_API_ENDPOINT), new Route(RestRequest.Method.POST, EXPLAIN_API_ENDPOINT));
    }

    @Override
    public String getName() {
        return "ml_ppl_query_action";
    }

    @Override
    protected Set<String> responseParams() {
        Set<String> responseParams = new HashSet<>(super.responseParams());
        responseParams.addAll(Arrays.asList("format", "sanitize"));
        return responseParams;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) {
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(PPLQueryRequestFactory.getPPLRequest(request));
        LOG.info("request classloader: " + transportPPLQueryRequest.getClass().getClassLoader());
        LOG.info("response classloader:" + TransportPPLQueryResponse.class.getClassLoader());

        return channel -> nodeClient
            .execute(PPLQueryAction.INSTANCE, transportPPLQueryRequest, getPPLTransportActionListener(new ActionListener<>() {
                @Override
                public void onResponse(TransportPPLQueryResponse response) {
                    sendResponse(channel, OK, response.getResult());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IllegalAccessException) {
                        LOG.error("Error happened during query handling", e);
                        reportError(channel, e, BAD_REQUEST);
                    } else if (transportPPLQueryRequest.isExplainRequest()) {
                        LOG.error("Error happened during explain", e);
                        sendResponse(channel, INTERNAL_SERVER_ERROR, "Failed to explain the query due to error: " + e.getMessage());
                    } else if (e instanceof OpenSearchSecurityException) {
                        OpenSearchSecurityException exception = (OpenSearchSecurityException) e;
                        reportError(channel, exception, exception.status());
                    } else {
                        LOG.error("Error happened during query handling", e);
                        reportError(channel, e, INTERNAL_SERVER_ERROR);
                    }
                }
            }));
    }

    private void sendResponse(RestChannel channel, RestStatus status, String content) {
        channel.sendResponse(new BytesRestResponse(status, "application/json; charset=UTF-8", content));
    }

    private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
        channel.sendResponse(new BytesRestResponse(status, e.getMessage()));
    }

    private <T extends ActionResponse> ActionListener<T> getPPLTransportActionListener(ActionListener<TransportPPLQueryResponse> listener) {
        return ActionListener.wrap(r -> { listener.onResponse(fromActionResponse(r)); }, listener::onFailure);
    }

    private static TransportPPLQueryResponse fromActionResponse(ActionResponse actionResponse) {
        if (actionResponse instanceof TransportPPLQueryResponse) {
            return (TransportPPLQueryResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new TransportPPLQueryResponse(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionResponse into TransportPPLQueryResponse", e);
        }

    }
}
