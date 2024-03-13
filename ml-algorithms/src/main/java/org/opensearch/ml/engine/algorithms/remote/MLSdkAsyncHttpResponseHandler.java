/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.ml.engine.algorithms.remote;

import static org.opensearch.ml.engine.algorithms.remote.ConnectorUtils.processErrorResponse;
import static org.opensearch.ml.engine.algorithms.remote.ConnectorUtils.processOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.ml.common.connector.Connector;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.script.ScriptService;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;

@Log4j2
public class MLSdkAsyncHttpResponseHandler implements SdkAsyncHttpResponseHandler {
    @Getter
    private Integer statusCode;
    @Getter
    private final StringBuilder responseBody = new StringBuilder();

    private WrappedCountDownLatch countDownLatch;

    private ActionListener<List<ModelTensors>> actionListener;

    private Map<String, String> parameters;

    private Map<Integer, ModelTensors> tensorOutputs;

    private Connector connector;

    private ScriptService scriptService;

    private final static Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public MLSdkAsyncHttpResponseHandler(
        WrappedCountDownLatch countDownLatch,
        ActionListener<List<ModelTensors>> actionListener,
        Map<String, String> parameters,
        Map<Integer, ModelTensors> tensorOutputs,
        Connector connector,
        ScriptService scriptService
    ) {
        this.countDownLatch = countDownLatch;
        this.actionListener = actionListener;
        this.parameters = parameters;
        this.tensorOutputs = tensorOutputs;
        this.connector = connector;
        this.scriptService = scriptService;
    }

    @Override
    public void onHeaders(SdkHttpResponse response) {
        SdkHttpFullResponse sdkResponse = (SdkHttpFullResponse) response;
        log.debug("received response headers: " + sdkResponse.headers());
        this.statusCode = sdkResponse.statusCode();
    }

    @Override
    public void onStream(Publisher<ByteBuffer> stream) {
        stream.subscribe(new MLResponseSubscriber());
    }

    @Override
    public void onError(Throwable error) {
        log.error(error.getMessage(), error);
        if (statusCode == null) {
            actionListener
                .onFailure(
                    new OpenSearchStatusException(
                        "Error on communication with remote model: " + error.getMessage(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
        } else {
            actionListener
                .onFailure(
                    new OpenSearchStatusException(
                        "Error on communication with remote model: " + error.getMessage(),
                        RestStatus.fromCode(statusCode)
                    )
                );
        }
    }

    private void processResponse(
        Integer statusCode,
        String body,
        Map<String, String> parameters,
        Map<Integer, ModelTensors> tensorOutputs
    ) {
        ModelTensors tensors;
        if (Strings.isBlank(body)) {
            log.error("Remote model response body is empty!");
            tensors = processErrorResponse("Remote model response is empty!");
        } else {
            if (statusCode < HttpStatus.SC_OK || statusCode > HttpStatus.SC_MULTIPLE_CHOICES) {
                log.error("Remote server returned error code: {}", statusCode);
                tensors = processErrorResponse(body);
            } else {
                try {
                    tensors = processOutput(body, connector, scriptService, parameters);
                } catch (Exception e) {
                    log.error("Failed to process response body: {}", body, e);
                    tensors = processErrorResponse(body);
                }
            }
        }
        tensors.setStatusCode(statusCode);
        tensorOutputs.put(countDownLatch.getSequence(), tensors);
    }

    private void reOrderTensorResponses(Map<Integer, ModelTensors> tensorOutputs) {
        List<ModelTensors> modelTensors = new ArrayList<>();
        TreeMap<Integer, ModelTensors> sortedMap = new TreeMap<>(tensorOutputs);
        log.debug("Reordered tensor outputs size is {}", sortedMap.size());
        if (tensorOutputs.size() == 1) {
            // batch API case
            ModelTensors singleTensor = tensorOutputs.get(0);
            int status = singleTensor.getStatusCode();
            if (status == HttpStatus.SC_OK) {
                modelTensors.add(singleTensor);
                actionListener.onResponse(modelTensors);
            } else {
                actionListener.onFailure(buildOpenSearchStatusException(singleTensor));
            }
        } else {
            // non batch API. This is to follow the previously code logic. Previously when making multiple requests to remote model,
            // either one fails, we will return a failure response.
            OpenSearchStatusException openSearchStatusException = null;
            for (Map.Entry<Integer, ModelTensors> entry : sortedMap.entrySet()) {
                if (entry.getValue().getStatusCode() < HttpStatus.SC_OK
                    || entry.getValue().getStatusCode() > HttpStatus.SC_MULTIPLE_CHOICES) {
                    openSearchStatusException = buildOpenSearchStatusException(entry.getValue());
                    break;
                }
                modelTensors.add(entry.getKey(), entry.getValue());
            }
            if (openSearchStatusException != null) {
                actionListener.onFailure(openSearchStatusException);
            } else {
                actionListener.onResponse(modelTensors);
            }
        }
    }

    private OpenSearchStatusException buildOpenSearchStatusException(ModelTensors modelTensors) {
        try {
            return new OpenSearchStatusException(
                AccessController
                    .doPrivileged(
                        (PrivilegedExceptionAction<String>) () -> GSON.toJson(modelTensors.getMlModelTensors().get(0).getDataAsMap())
                    ),
                RestStatus.fromCode(modelTensors.getStatusCode())
            );
        } catch (PrivilegedActionException e) {
            return new OpenSearchStatusException(e.getMessage(), RestStatus.fromCode(statusCode));
        }
    }

    protected class MLResponseSubscriber implements Subscriber<ByteBuffer> {
        private Subscription subscription;

        @Override
        public void onSubscribe(Subscription s) {
            this.subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer) {
            responseBody.append(StandardCharsets.UTF_8.decode(byteBuffer));
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onError(Throwable t) {
            log
                .error(
                    "Error on receiving response body from remote: {}",
                    t instanceof NullPointerException ? "NullPointerException" : t.getMessage(),
                    t
                );
            processResponse(statusCode, responseBody.toString(), parameters, tensorOutputs);
            countDownLatch.getCountDownLatch().countDown();
            response(tensorOutputs);
        }

        @Override
        public void onComplete() {
            processResponse(statusCode, responseBody.toString(), parameters, tensorOutputs);
            countDownLatch.getCountDownLatch().countDown();
            response(tensorOutputs);
        }
    }

    private void response(Map<Integer, ModelTensors> tensors) {
        // when countdown's count equals to 0 means all responses are received.
        if (countDownLatch.getCountDownLatch().getCount() == 0) {
            reOrderTensorResponses(tensors);
        } else {
            log.debug("Not all responses received, left response count is: " + countDownLatch.getCountDownLatch().getCount());
        }
    }
}
