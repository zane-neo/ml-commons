/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.common.xcontent.XContentType.JSON;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.ml.common.CommonValue.ML_MODEL_GROUP_INDEX;
import static org.opensearch.ml.common.CommonValue.ML_MODEL_INDEX;
import static org.opensearch.ml.common.CommonValue.NOT_FOUND;
import static org.opensearch.ml.common.CommonValue.UNDEPLOYED;
import static org.opensearch.ml.common.MLModel.ALGORITHM_FIELD;
import static org.opensearch.ml.common.MLTask.ERROR_FIELD;
import static org.opensearch.ml.common.MLTask.MODEL_ID_FIELD;
import static org.opensearch.ml.common.MLTask.STATE_FIELD;
import static org.opensearch.ml.common.MLTaskState.COMPLETED;
import static org.opensearch.ml.common.MLTaskState.FAILED;
import static org.opensearch.ml.engine.ModelHelper.CHUNK_FILES;
import static org.opensearch.ml.engine.ModelHelper.MODEL_FILE_HASH;
import static org.opensearch.ml.engine.ModelHelper.MODEL_SIZE_IN_BYTES;
import static org.opensearch.ml.engine.algorithms.DLModel.ML_ENGINE;
import static org.opensearch.ml.engine.algorithms.DLModel.MODEL_HELPER;
import static org.opensearch.ml.engine.algorithms.DLModel.MODEL_ZIP_FILE;
import static org.opensearch.ml.engine.utils.FileUtils.calculateFileHash;
import static org.opensearch.ml.engine.utils.FileUtils.deleteFileQuietly;
import static org.opensearch.ml.plugin.MachineLearningPlugin.DEPLOY_THREAD_POOL;
import static org.opensearch.ml.plugin.MachineLearningPlugin.REGISTER_THREAD_POOL;
import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_MAX_DEPLOY_MODEL_TASKS_PER_NODE;
import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_MAX_MODELS_PER_NODE;
import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_MAX_REGISTER_MODEL_TASKS_PER_NODE;
import static org.opensearch.ml.stats.ActionName.REGISTER;
import static org.opensearch.ml.stats.MLActionLevelStat.ML_ACTION_REQUEST_COUNT;
import static org.opensearch.ml.utils.MLExceptionUtils.logException;
import static org.opensearch.ml.utils.MLNodeUtils.checkOpenCircuitBreaker;
import static org.opensearch.ml.utils.MLNodeUtils.createXContentParserFromRegistry;

import java.io.File;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.ml.breaker.MLCircuitBreakerService;
import org.opensearch.ml.cluster.DiscoveryNodeHelper;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.MLModelGroup;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.exception.MLException;
import org.opensearch.ml.common.exception.MLResourceNotFoundException;
import org.opensearch.ml.common.exception.MLValidationException;
import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.ml.common.transport.deploy.MLDeployModelAction;
import org.opensearch.ml.common.transport.deploy.MLDeployModelRequest;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.upload_chunk.MLRegisterModelMetaInput;
import org.opensearch.ml.engine.MLEngine;
import org.opensearch.ml.engine.MLExecutable;
import org.opensearch.ml.engine.ModelHelper;
import org.opensearch.ml.engine.Predictable;
import org.opensearch.ml.engine.utils.FileUtils;
import org.opensearch.ml.indices.MLIndicesHandler;
import org.opensearch.ml.profile.MLModelProfile;
import org.opensearch.ml.stats.ActionName;
import org.opensearch.ml.stats.MLActionLevelStat;
import org.opensearch.ml.stats.MLNodeLevelStat;
import org.opensearch.ml.stats.MLStats;
import org.opensearch.ml.task.MLTaskManager;
import org.opensearch.ml.utils.MLExceptionUtils;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

/**
 * Manager class for ML models. It contains ML model related operations like register, deploy model etc.
 */
@Log4j2
public class MLModelManager {

    public static final int TIMEOUT_IN_MILLIS = 5000;
    public static final long MODEL_FILE_SIZE_LIMIT = 4l * 1024 * 1024 * 1024;// 4GB

    private final Client client;
    private final ClusterService clusterService;
    private ThreadPool threadPool;
    private NamedXContentRegistry xContentRegistry;
    private ModelHelper modelHelper;

    private final MLModelCacheHelper modelCacheHelper;
    private final MLStats mlStats;
    private final MLCircuitBreakerService mlCircuitBreakerService;
    private final MLIndicesHandler mlIndicesHandler;
    private final MLTaskManager mlTaskManager;
    private final MLEngine mlEngine;
    private final DiscoveryNodeHelper nodeHelper;

    private volatile Integer maxModelPerNode;
    private volatile Integer maxRegisterTasksPerNode;
    private volatile Integer maxDeployTasksPerNode;

    public static final ImmutableSet MODEL_DONE_STATES = ImmutableSet
        .of(
            MLModelState.TRAINED,
            MLModelState.REGISTERED,
            MLModelState.DEPLOYED,
            MLModelState.PARTIALLY_DEPLOYED,
            MLModelState.DEPLOY_FAILED,
            MLModelState.UNDEPLOYED
        );

    public MLModelManager(
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ModelHelper modelHelper,
        Settings settings,
        MLStats mlStats,
        MLCircuitBreakerService mlCircuitBreakerService,
        MLIndicesHandler mlIndicesHandler,
        MLTaskManager mlTaskManager,
        MLModelCacheHelper modelCacheHelper,
        MLEngine mlEngine,
        DiscoveryNodeHelper nodeHelper
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.modelHelper = modelHelper;
        this.clusterService = clusterService;
        this.modelCacheHelper = modelCacheHelper;
        this.mlStats = mlStats;
        this.mlCircuitBreakerService = mlCircuitBreakerService;
        this.mlIndicesHandler = mlIndicesHandler;
        this.mlTaskManager = mlTaskManager;
        this.mlEngine = mlEngine;
        this.nodeHelper = nodeHelper;

        this.maxModelPerNode = ML_COMMONS_MAX_MODELS_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ML_COMMONS_MAX_MODELS_PER_NODE, it -> maxModelPerNode = it);

        maxRegisterTasksPerNode = ML_COMMONS_MAX_REGISTER_MODEL_TASKS_PER_NODE.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(ML_COMMONS_MAX_REGISTER_MODEL_TASKS_PER_NODE, it -> maxRegisterTasksPerNode = it);

        maxDeployTasksPerNode = ML_COMMONS_MAX_DEPLOY_MODEL_TASKS_PER_NODE.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(ML_COMMONS_MAX_DEPLOY_MODEL_TASKS_PER_NODE, it -> maxDeployTasksPerNode = it);
    }

    public void registerModelMeta(MLRegisterModelMetaInput mlRegisterModelMetaInput, ActionListener<String> listener) {
        try {
            FunctionName functionName = mlRegisterModelMetaInput.getFunctionName();
            mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_REQUEST_COUNT).increment();
            mlStats.createCounterStatIfAbsent(functionName, REGISTER, ML_ACTION_REQUEST_COUNT).increment();
            String modelName = mlRegisterModelMetaInput.getName();
            String version = mlRegisterModelMetaInput.getVersion();
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                mlIndicesHandler.initModelIndexIfAbsent(ActionListener.wrap(res -> {
                    Instant now = Instant.now();
                    MLModel mlModelMeta = MLModel
                        .builder()
                        .name(modelName)
                        .algorithm(functionName)
                        .version(version)
                        .description(mlRegisterModelMetaInput.getDescription())
                        .modelFormat(mlRegisterModelMetaInput.getModelFormat())
                        .modelState(MLModelState.REGISTERING)
                        .modelConfig(mlRegisterModelMetaInput.getModelConfig())
                        .totalChunks(mlRegisterModelMetaInput.getTotalChunks())
                        .modelContentHash(mlRegisterModelMetaInput.getModelContentHashValue())
                        .modelContentSizeInBytes(mlRegisterModelMetaInput.getModelContentSizeInBytes())
                        .createdTime(now)
                        .lastUpdateTime(now)
                        .build();
                    IndexRequest indexRequest = new IndexRequest(ML_MODEL_INDEX);
                    indexRequest.source(mlModelMeta.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), EMPTY_PARAMS));
                    indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                    client.index(indexRequest, ActionListener.wrap(r -> {
                        log.debug("Index model meta doc successfully {}", modelName);
                        listener.onResponse(r.getId());
                    }, e -> {
                        log.error("Failed to index model meta doc", e);
                        listener.onFailure(e);
                    }));
                }, ex -> {
                    log.error("Failed to init model index", ex);
                    listener.onFailure(ex);
                }));
            } catch (Exception e) {
                log.error("Failed to register model meta doc", e);
                listener.onFailure(e);
            }
        } catch (final Exception e) {
            log.error("Failed to init model index", e);
            listener.onFailure(e);
        }
    }

    /**
     * Register model. Basically download model file, split into chunks and save into model index.
     *
     * @param registerModelInput register model input
     * @param mlTask      ML task
     */
    public void registerMLModel(MLRegisterModelInput registerModelInput, MLTask mlTask) {
        mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_REQUEST_COUNT).increment();
        checkAndAddRunningTask(mlTask, maxRegisterTasksPerNode);
        try {
            mlStats.getStat(MLNodeLevelStat.ML_NODE_EXECUTING_TASK_COUNT).increment();
            mlStats.createCounterStatIfAbsent(mlTask.getFunctionName(), REGISTER, ML_ACTION_REQUEST_COUNT).increment();

            String modelGroupId = registerModelInput.getModelGroupId();
            GetRequest getModelGroupRequest = new GetRequest(ML_MODEL_GROUP_INDEX).id(modelGroupId);
            if (modelGroupId != null) {
                try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                    client.get(getModelGroupRequest, ActionListener.wrap(modelGroup -> {
                        if (modelGroup.isExists()) {
                            Map<String, Object> source = modelGroup.getSourceAsMap();
                            int latestVersion = (int) source.get(MLModelGroup.LATEST_VERSION_FIELD);
                            int newVersion = latestVersion + 1;
                            source.put(MLModelGroup.LATEST_VERSION_FIELD, newVersion);
                            source.put(MLModelGroup.LAST_UPDATED_TIME_FIELD, Instant.now().toEpochMilli());
                            UpdateRequest updateModelGroupRequest = new UpdateRequest();
                            long seqNo = modelGroup.getSeqNo();
                            long primaryTerm = modelGroup.getPrimaryTerm();
                            updateModelGroupRequest
                                .index(ML_MODEL_GROUP_INDEX)
                                .id(modelGroupId)
                                .setIfSeqNo(seqNo)
                                .setIfPrimaryTerm(primaryTerm)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                .doc(source);
                            client
                                .update(
                                    updateModelGroupRequest,
                                    ActionListener
                                        .wrap(
                                            r -> { uploadModel(registerModelInput, mlTask, newVersion + ""); },
                                            e -> {
                                                log.error("Failed to update model group", e);
                                                handleException(registerModelInput.getFunctionName(), mlTask.getTaskId(), e);
                                            }
                                        )
                                );
                        } else {
                            log.error("Model group not found");
                            handleException(
                                registerModelInput.getFunctionName(),
                                mlTask.getTaskId(),
                                new MLValidationException("Model group not found")
                            );
                        }
                    }, e -> {
                        log.error("Failed to get model group", e);
                        handleException(registerModelInput.getFunctionName(), mlTask.getTaskId(), e);
                    }));
                } catch (Exception e) {
                    log.error("Failed to register model", e);
                    handleException(registerModelInput.getFunctionName(), mlTask.getTaskId(), e);
                }
            } else {
                // TODO Do we need to support register model without model group id?
                uploadModel(registerModelInput, mlTask, null);
            }
        } catch (Exception e) {
            mlStats.createCounterStatIfAbsent(mlTask.getFunctionName(), REGISTER, MLActionLevelStat.ML_ACTION_FAILURE_COUNT).increment();
            handleException(registerModelInput.getFunctionName(), mlTask.getTaskId(), e);
        } finally {
            mlStats.getStat(MLNodeLevelStat.ML_NODE_EXECUTING_TASK_COUNT).increment();
        }
    }

    private void uploadModel(MLRegisterModelInput registerModelInput, MLTask mlTask, String modelVersion)
        throws PrivilegedActionException {
        if (registerModelInput.getUrl() != null) {
            registerModelFromUrl(registerModelInput, mlTask, modelVersion);
        } else {
            registerPrebuiltModel(registerModelInput, mlTask, modelVersion);
        }
    }

    private void registerModelFromUrl(
        MLRegisterModelInput registerModelInput,
        MLTask mlTask,
        String modelVersion
    ) {
        String taskId = mlTask.getTaskId();
        FunctionName functionName = mlTask.getFunctionName();
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_REQUEST_COUNT).increment();

            mlStats.createCounterStatIfAbsent(functionName, REGISTER, ML_ACTION_REQUEST_COUNT).increment();
            mlStats.getStat(MLNodeLevelStat.ML_NODE_EXECUTING_TASK_COUNT).increment();
            String modelName = registerModelInput.getModelName();
            String version = modelVersion == null ? registerModelInput.getVersion() : modelVersion;
            String modelGroupId = registerModelInput.getModelGroupId();
            Instant now = Instant.now();
            mlIndicesHandler.initModelIndexIfAbsent(ActionListener.wrap(res -> {
                MLModel mlModelMeta = MLModel
                    .builder()
                    .name(modelName)
                    .modelGroupId(modelGroupId)
                    .algorithm(functionName)
                    .version(version)
                    .description(registerModelInput.getDescription())
                    .modelFormat(registerModelInput.getModelFormat())
                    .modelState(MLModelState.REGISTERING)
                    .modelConfig(registerModelInput.getModelConfig())
                    .createdTime(now)
                    .lastUpdateTime(now)
                    .build();
                IndexRequest indexModelMetaRequest = new IndexRequest(ML_MODEL_INDEX);
                indexModelMetaRequest.source(mlModelMeta.toXContent(XContentBuilder.builder(JSON.xContent()), EMPTY_PARAMS));
                indexModelMetaRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                // create model meta doc
                ActionListener<IndexResponse> listener = ActionListener.wrap(modelMetaRes -> {
                    String modelId = modelMetaRes.getId();
                    mlTask.setModelId(modelId);
                    log.info("create new model meta doc {} for register model task {}", modelId, taskId);
                    // model group id is not present in request body.
                    registerModel(registerModelInput, taskId, functionName, modelName, version, modelId);
                }, e -> {
                    log.error("Failed to index model meta doc", e);
                    handleException(functionName, taskId, e);
                });

                client.index(indexModelMetaRequest, threadedActionListener(REGISTER_THREAD_POOL, listener));
            }, e -> {
                log.error("Failed to init model index", e);
                handleException(functionName, taskId, e);
            }));
        } catch (Exception e) {
            logException("Failed to register model", e, log);
            handleException(functionName, taskId, e);
        } finally {
            mlStats.getStat(MLNodeLevelStat.ML_NODE_EXECUTING_TASK_COUNT).increment();
        }
    }

    private void registerModel(
        MLRegisterModelInput registerModelInput,
        String taskId,
        FunctionName functionName,
        String modelName,
        String version,
        String modelId
    ) {
        modelHelper
            .downloadAndSplit(
                registerModelInput.getModelFormat(),
                modelId,
                modelName,
                version,
                registerModelInput.getUrl(),
                registerModelInput.getHashValue(),
                ActionListener.wrap(result -> {
                    Long modelSizeInBytes = (Long) result.get(MODEL_SIZE_IN_BYTES);
                    if (modelSizeInBytes >= MODEL_FILE_SIZE_LIMIT) {
                        throw new MLException("Model file size exceeds the limit of 4GB: " + modelSizeInBytes);
                    }
                    List<String> chunkFiles = (List<String>) result.get(CHUNK_FILES);
                    String hashValue = (String) result.get(MODEL_FILE_HASH);
                    Semaphore semaphore = new Semaphore(1);
                    AtomicInteger uploaded = new AtomicInteger(0);
                    AtomicBoolean failedToUploadChunk = new AtomicBoolean(false);
                    // upload chunks
                    for (String name : chunkFiles) {
                        semaphore.tryAcquire(10, TimeUnit.SECONDS);
                        if (failedToUploadChunk.get()) {
                            throw new MLException("Failed to save model chunk");
                        }
                        File file = new File(name);
                        byte[] bytes = Files.toByteArray(file);
                        int chunkNum = Integer.parseInt(file.getName());
                        Instant now = Instant.now();
                        MLModel mlModel = MLModel
                            .builder()
                            .modelId(modelId)
                            .name(modelName)
                            .algorithm(functionName)
                            .version(version)
                            .modelFormat(registerModelInput.getModelFormat())
                            .chunkNumber(chunkNum)
                            .totalChunks(chunkFiles.size())
                            .content(Base64.getEncoder().encodeToString(bytes))
                            .createdTime(now)
                            .lastUpdateTime(now)
                            .build();
                        IndexRequest indexRequest = new IndexRequest(ML_MODEL_INDEX);
                        String chunkId = getModelChunkId(modelId, chunkNum);
                        indexRequest.id(chunkId);
                        indexRequest.source(mlModel.toXContent(XContentBuilder.builder(JSON.xContent()), EMPTY_PARAMS));
                        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                        client.index(indexRequest, ActionListener.wrap(r -> {
                            uploaded.getAndIncrement();
                            if (uploaded.get() == chunkFiles.size()) {
                                updateModelRegisterStateAsDone(
                                    registerModelInput,
                                    taskId,
                                    modelId,
                                    modelSizeInBytes,
                                    chunkFiles,
                                    hashValue
                                );
                            } else {
                                deleteFileQuietly(file);
                            }
                            semaphore.release();
                        }, e -> {
                            log.error("Failed to index model chunk " + chunkId, e);
                            failedToUploadChunk.set(true);
                            handleException(functionName, taskId, e);
                            deleteFileQuietly(file);
                            // remove model doc as failed to upload model
                            deleteModel(modelId);
                            semaphore.release();
                            deleteFileQuietly(mlEngine.getRegisterModelPath(modelId));
                        }));
                    }
                }, e -> {
                    log.error("Failed to index chunk file", e);
                    deleteFileQuietly(mlEngine.getRegisterModelPath(modelId));
                    deleteModel(modelId);
                    handleException(functionName, taskId, e);
                })
            );
    }

    private void registerPrebuiltModel(
        MLRegisterModelInput registerModelInput,
        MLTask mlTask,
        String modelVersion
    ) throws PrivilegedActionException {
        String taskId = mlTask.getTaskId();
        List modelMetaList = modelHelper.downloadPrebuiltModelMetaList(taskId, registerModelInput);
        if (!modelHelper.isModelAllowed(registerModelInput, modelMetaList)) {
            throw new IllegalArgumentException("This model is not in the pre-trained model list, please check your parameters.");
        }
        modelHelper
            .downloadPrebuiltModelConfig(
                taskId,
                registerModelInput,
                ActionListener
                    .wrap(
                        mlRegisterModelInput -> { registerModelFromUrl(mlRegisterModelInput, mlTask, modelVersion); },
                        e -> {
                            log.error("Failed to register prebuilt model", e);
                            handleException(registerModelInput.getFunctionName(), taskId, e);
                        }
                    )
            );
    }

    private <T> ThreadedActionListener<T> threadedActionListener(String threadPoolName, ActionListener<T> listener) {
        return new ThreadedActionListener<>(log, threadPool, threadPoolName, listener, false);
    }

    /**
     * Check if exceed running task limit and if circuit breaker is open.
     * @param mlTask ML task
     * @param runningTaskLimit limit
     */
    public void checkAndAddRunningTask(MLTask mlTask, Integer runningTaskLimit) {
        checkOpenCircuitBreaker(mlCircuitBreakerService, mlStats);
        mlTaskManager.checkLimitAndAddRunningTask(mlTask, runningTaskLimit);
    }

    private void updateModelRegisterStateAsDone(
        MLRegisterModelInput registerModelInput,
        String taskId,
        String modelId,
        Long modelSizeInBytes,
        List<String> chunkFiles,
        String hashValue
    ) {
        FunctionName functionName = registerModelInput.getFunctionName();
        deleteFileQuietly(mlEngine.getRegisterModelPath(modelId));
        Map<String, Object> updatedFields = ImmutableMap
            .of(
                MLModel.MODEL_STATE_FIELD,
                MLModelState.REGISTERED,
                MLModel.LAST_REGISTERED_TIME_FIELD,
                Instant.now().toEpochMilli(),
                MLModel.TOTAL_CHUNKS_FIELD,
                chunkFiles.size(),
                MLModel.MODEL_CONTENT_HASH_VALUE_FIELD,
                hashValue,
                MLModel.MODEL_CONTENT_SIZE_IN_BYTES_FIELD,
                modelSizeInBytes
            );
        log.info("Model registered successfully, model id: {}, task id: {}", modelId, taskId);
        updateModel(modelId, updatedFields, ActionListener.wrap(updateResponse -> {
            mlTaskManager.updateMLTask(taskId, ImmutableMap.of(STATE_FIELD, COMPLETED, MODEL_ID_FIELD, modelId), TIMEOUT_IN_MILLIS, true);
            if (registerModelInput.isDeployModel()) {
                deployModelAfterRegistering(registerModelInput, modelId);
            }
        }, e -> {
            log.error("Failed to update model", e);
            handleException(functionName, taskId, e);
            deleteModel(modelId);
        }));
    }

    private void deployModelAfterRegistering(MLRegisterModelInput registerModelInput, String modelId) {
        String[] modelNodeIds = registerModelInput.getModelNodeIds();
        log.debug("start deploying model after registering, modelId: {} on nodes: {}", modelId, Arrays.toString(modelNodeIds));
        MLDeployModelRequest request = new MLDeployModelRequest(modelId, modelNodeIds, false, true);
        ActionListener<MLDeployModelResponse> listener = ActionListener
            .wrap(r -> log.debug("model deployed, response {}", r), e -> log.error("Failed to deploy model", e));
        client.execute(MLDeployModelAction.INSTANCE, request, listener);
    }

    private void deleteModel(String modelId) {
        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index(ML_MODEL_INDEX).id(modelId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest);
        DeleteByQueryRequest deleteChunksRequest = new DeleteByQueryRequest(ML_MODEL_INDEX)
            .setQuery(new TermQueryBuilder(MLModel.MODEL_ID_FIELD, modelId))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setAbortOnVersionConflict(false);
        client.execute(DeleteByQueryAction.INSTANCE, deleteChunksRequest);
    }

    private void handleException(FunctionName functionName, String taskId, Exception e) {
        mlStats.createCounterStatIfAbsent(functionName, REGISTER, MLActionLevelStat.ML_ACTION_FAILURE_COUNT).increment();
        Map<String, Object> updated = ImmutableMap.of(ERROR_FIELD, MLExceptionUtils.getRootCauseMessage(e), STATE_FIELD, FAILED);
        mlTaskManager.updateMLTask(taskId, updated, TIMEOUT_IN_MILLIS, true);
    }

    /**
     * Read model chunks from model index. Concat chunks into a whole model file, then load
     * into memory.
     *
     * @param modelId          model id
     * @param modelContentHash model content hash value
     * @param functionName     function name
     * @param mlTask           ML task
     * @param listener         action listener
     */
    public void deployModel(
        String modelId,
        String modelContentHash,
        FunctionName functionName,
        boolean deployToAllNodes,
        MLTask mlTask,
        ActionListener<String> listener
    ) {
        mlStats.createCounterStatIfAbsent(functionName, ActionName.DEPLOY, ML_ACTION_REQUEST_COUNT).increment();
        List<String> workerNodes = mlTask.getWorkerNodes();
        if (modelCacheHelper.isModelDeployed(modelId)) {
            if (workerNodes != null && workerNodes.size() > 0) {
                log.info("Set new target node ids {} for model {}", Arrays.toString(workerNodes.toArray(new String[0])), modelId);
                modelCacheHelper.setDeployToAllNodes(modelId, deployToAllNodes);
                modelCacheHelper.setTargetWorkerNodes(modelId, workerNodes);
            }
            listener.onResponse("successful");
            return;
        }
        if (modelCacheHelper.getDeployedModels().length >= maxModelPerNode) {
            listener.onFailure(new IllegalArgumentException("Exceed max model per node limit"));
            return;
        }
        modelCacheHelper.initModelState(modelId, MLModelState.DEPLOYING, functionName, workerNodes, deployToAllNodes);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            checkAndAddRunningTask(mlTask, maxDeployTasksPerNode);
            this.getModel(modelId, threadedActionListener(DEPLOY_THREAD_POOL, ActionListener.wrap(mlModel -> {
                if (!FunctionName.isDLModel(mlModel.getAlgorithm()) && mlModel.getAlgorithm() != FunctionName.METRICS_CORRELATION) {
                    // deploy model trained by built-in algorithm like kmeans
                    Predictable predictable = mlEngine.deploy(mlModel, null);
                    modelCacheHelper.setPredictor(modelId, predictable);
                    mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_MODEL_COUNT).increment();
                    modelCacheHelper.setModelState(modelId, MLModelState.DEPLOYED);
                    listener.onResponse("successful");
                    return;
                }
                // check circuit breaker before deploying custom model chunks
                checkOpenCircuitBreaker(mlCircuitBreakerService, mlStats);
                retrieveModelChunks(mlModel, ActionListener.wrap(modelZipFile -> {// read model chunks
                    String hash = calculateFileHash(modelZipFile);
                    if (modelContentHash != null && !modelContentHash.equals(hash)) {
                        log.error("Model content hash can't match original hash value");
                        removeModel(modelId);
                        listener.onFailure(new IllegalArgumentException("model content changed"));
                        return;
                    }
                    log.debug("Model content matches original hash value, continue deploying");
                    Map<String, Object> params = ImmutableMap
                        .of(MODEL_ZIP_FILE, modelZipFile, MODEL_HELPER, modelHelper, ML_ENGINE, mlEngine);
                    if (FunctionName.METRICS_CORRELATION.equals(mlModel.getAlgorithm())) {
                        MLExecutable mlExecutable = mlEngine.deployExecute(mlModel, params);
                        try {
                            modelCacheHelper.setMLExecutor(modelId, mlExecutable);
                            mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_MODEL_COUNT).increment();
                            modelCacheHelper.setModelState(modelId, MLModelState.DEPLOYED);
                            listener.onResponse("successful");
                        } catch (Exception e) {
                            log.error("Failed to add predictor to cache", e);
                            mlExecutable.close();
                            listener.onFailure(e);
                        }

                    } else {
                        Predictable predictable = mlEngine.deploy(mlModel, params);
                        try {
                            modelCacheHelper.setPredictor(modelId, predictable);
                            mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_MODEL_COUNT).increment();
                            modelCacheHelper.setModelState(modelId, MLModelState.DEPLOYED);
                            modelCacheHelper.setMemSizeEstimation(modelId, mlModel.getModelFormat(), mlModel.getModelContentSizeInBytes());
                            listener.onResponse("successful");
                        } catch (Exception e) {
                            log.error("Failed to add predictor to cache", e);
                            predictable.close();
                            listener.onFailure(e);
                        }
                    }
                }, e -> {
                    log.error("Failed to retrieve model " + modelId, e);
                    handleDeployModelException(modelId, functionName, listener, e);
                }));
            }, e -> {
                log.error("Failed to deploy model " + modelId, e);
                handleDeployModelException(modelId, functionName, listener, e);
            })));
        } catch (Exception e) {
            handleDeployModelException(modelId, functionName, listener, e);
        }
    }

    private void handleDeployModelException(String modelId, FunctionName functionName, ActionListener<String> listener, Exception e) {
        mlStats.createCounterStatIfAbsent(functionName, ActionName.DEPLOY, MLActionLevelStat.ML_ACTION_FAILURE_COUNT).increment();
        removeModel(modelId);
        listener.onFailure(e);
    }

    /**
     * Get model from model index.
     *
     * @param modelId  model id
     * @param listener action listener
     */
    public void getModel(String modelId, ActionListener<MLModel> listener) {
        getModel(modelId, null, null, listener);
    }

    /**
     * Get model from model index with includes/exludes filter.
     *
     * @param modelId  model id
     * @param includes fields included
     * @param excludes fields excluded
     * @param listener action listener
     */
    public void getModel(String modelId, String[] includes, String[] excludes, ActionListener<MLModel> listener) {
        GetRequest getRequest = new GetRequest();
        FetchSourceContext fetchContext = new FetchSourceContext(true, includes, excludes);
        getRequest.index(ML_MODEL_INDEX).id(modelId).fetchSourceContext(fetchContext);
        client.get(getRequest, ActionListener.wrap(r -> {
            if (r != null && r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    GetResponse getResponse = r;
                    String algorithmName = getResponse.getSource().get(ALGORITHM_FIELD).toString();

                    MLModel mlModel = MLModel.parse(parser, algorithmName);
                    mlModel.setModelId(modelId);
                    listener.onResponse(mlModel);
                } catch (Exception e) {
                    log.error("Failed to parse ml task" + r.getId(), e);
                    listener.onFailure(e);
                }
            } else {
                listener.onFailure(new MLResourceNotFoundException("Fail to find model"));
            }
        }, e -> { listener.onFailure(e); }));
    }

    private void retrieveModelChunks(MLModel mlModelMeta, ActionListener<File> listener) throws InterruptedException {
        String modelId = mlModelMeta.getModelId();
        String modelName = mlModelMeta.getName();
        Integer totalChunks = mlModelMeta.getTotalChunks();
        GetRequest getRequest = new GetRequest();
        getRequest.index(ML_MODEL_INDEX);
        getRequest.id();
        Semaphore semaphore = new Semaphore(1);
        AtomicBoolean stopNow = new AtomicBoolean(false);
        String modelZip = mlEngine.getDeployModelZipPath(modelId, modelName);
        ConcurrentLinkedDeque<File> chunkFiles = new ConcurrentLinkedDeque();
        AtomicInteger retrievedChunks = new AtomicInteger(0);
        for (int i = 0; i < totalChunks; i++) {
            semaphore.tryAcquire(10, TimeUnit.SECONDS);
            if (stopNow.get()) {
                throw new MLException("Failed to deploy model");
            }
            String modelChunkId = this.getModelChunkId(modelId, i);
            int currentChunk = i;
            this.getModel(modelChunkId, threadedActionListener(DEPLOY_THREAD_POOL, ActionListener.wrap(model -> {
                Path chunkPath = mlEngine.getDeployModelChunkPath(modelId, currentChunk);
                FileUtils.write(Base64.getDecoder().decode(model.getContent()), chunkPath.toString());
                chunkFiles.add(new File(chunkPath.toUri()));
                retrievedChunks.getAndIncrement();
                if (retrievedChunks.get() == totalChunks) {
                    File modelZipFile = new File(modelZip);
                    FileUtils.mergeFiles(chunkFiles, modelZipFile);
                    listener.onResponse(modelZipFile);
                }
                semaphore.release();
            }, e -> {
                stopNow.set(true);
                semaphore.release();
                log.error("Failed to retrieve model chunk " + modelChunkId, e);
                if (retrievedChunks.get() == totalChunks - 1) {
                    listener.onFailure(new MLResourceNotFoundException("Fail to find model chunk " + modelChunkId));
                }
            })));
        }
    }

    /**
     * Update model with build-in listener.
     *
     * @param modelId       model id
     * @param updatedFields updated fields
     */
    public void updateModel(String modelId, Map<String, Object> updatedFields) {
        updateModel(modelId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                log.debug("Updated ML model successfully: {}, model id: {}", response.status(), modelId);
            } else {
                log.error("Failed to update ML model {}, status: {}", modelId, response.status());
            }
        }, e -> { log.error("Failed to update ML model: " + modelId, e); }));
    }

    /**
     * Update model.
     *
     * @param modelId       model id
     * @param updatedFields updated fields
     * @param listener      action listener
     */
    public void updateModel(String modelId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        if (updatedFields == null || updatedFields.size() == 0) {
            listener.onFailure(new IllegalArgumentException("Updated fields is null or empty"));
            return;
        }
        Map<String, Object> newUpdatedFields = new HashMap<>();
        newUpdatedFields.putAll(updatedFields);
        newUpdatedFields.put(MLModel.LAST_UPDATED_TIME_FIELD, Instant.now().toEpochMilli());
        UpdateRequest updateRequest = new UpdateRequest(ML_MODEL_INDEX, modelId);
        updateRequest.doc(newUpdatedFields);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        if (newUpdatedFields.containsKey(MLModel.MODEL_STATE_FIELD)
            && MODEL_DONE_STATES.contains(newUpdatedFields.get(MLModel.MODEL_STATE_FIELD))) {
            updateRequest.retryOnConflict(3);
        }
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.update(updateRequest, ActionListener.runBefore(listener, () -> context.restore()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Get model chunk id
     * @param modelId model id
     * @param chunkNumber model chunk number
     * @return model chunk id
     */
    public String getModelChunkId(String modelId, Integer chunkNumber) {
        return modelId + "_" + chunkNumber;
    }

    /**
     * Add model worker node to cache.
     *
     * @param modelId model id
     * @param nodeIds node ids
     */
    public void addModelWorkerNode(String modelId, String... nodeIds) {
        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                modelCacheHelper.addWorkerNode(modelId, nodeId);
            }
        }
    }

    public void addModelWorkerNodes(List<String> nodeIds) {
        if (nodeIds != null) {
            String[] modelIds = getAllModelIds();
            for (String nodeId : nodeIds) {
                Arrays.stream(modelIds).forEach(x -> modelCacheHelper.addWorkerNode(x, nodeId));
            }
        }
    }

    /**
     * Remove model from worker node cache.
     *
     * @param modelId model id
     * @param nodeIds node ids
     */
    public void removeModelWorkerNode(String modelId, boolean isFromUndeploy, String... nodeIds) {
        if (nodeIds != null) {
            for (String nodeId : nodeIds) {
                modelCacheHelper.removeWorkerNode(modelId, nodeId, isFromUndeploy);
            }
        }
    }

    /**
     * Remove a set of worker nodes from cache.
     *
     * @param removedNodes removed node ids
     */
    public void removeWorkerNodes(Set<String> removedNodes, boolean isFromUndeploy) {
        modelCacheHelper.removeWorkerNodes(removedNodes, isFromUndeploy);
    }

    /**
     * Undeploy model from memory.
     *
     * @param modelIds model ids
     * @return model undeploy status
     */
    public synchronized Map<String, String> undeployModel(String[] modelIds) {
        Map<String, String> modelUndeployStatus = new HashMap<>();
        if (modelIds != null && modelIds.length > 0) {
            log.debug("undeploy models {}", Arrays.toString(modelIds));
            for (String modelId : modelIds) {
                if (modelCacheHelper.isModelDeployed(modelId)) {
                    modelUndeployStatus.put(modelId, UNDEPLOYED);
                    mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_MODEL_COUNT).decrement();
                    mlStats
                        .createCounterStatIfAbsent(getModelFunctionName(modelId), ActionName.UNDEPLOY, ML_ACTION_REQUEST_COUNT)
                        .increment();
                } else {
                    modelUndeployStatus.put(modelId, NOT_FOUND);
                }
                removeModel(modelId);
            }
        } else {
            log.debug("undeploy all models {}", Arrays.toString(getLocalDeployedModels()));
            for (String modelId : getLocalDeployedModels()) {
                modelUndeployStatus.put(modelId, UNDEPLOYED);
                mlStats.getStat(MLNodeLevelStat.ML_NODE_TOTAL_MODEL_COUNT).decrement();
                mlStats.createCounterStatIfAbsent(getModelFunctionName(modelId), ActionName.UNDEPLOY, ML_ACTION_REQUEST_COUNT).increment();
                removeModel(modelId);
            }
        }
        return modelUndeployStatus;
    }

    private void removeModel(String modelId) {
        modelCacheHelper.removeModel(modelId);
        modelHelper.deleteFileCache(modelId);
    }

    /**
     * Get worker nodes of specific model.
     *
     * @param modelId model id
     * @param onlyEligibleNode return only eligible node
     * @return list of worker node ids
     */
    public String[] getWorkerNodes(String modelId, boolean onlyEligibleNode) {
        String[] workerNodeIds = modelCacheHelper.getWorkerNodes(modelId);
        if (!onlyEligibleNode) {
            return workerNodeIds;
        }
        if (workerNodeIds == null || workerNodeIds.length == 0) {
            return workerNodeIds;
        }

        String[] eligibleNodeIds = nodeHelper.filterEligibleNodes(workerNodeIds);
        if (eligibleNodeIds == null || eligibleNodeIds.length == 0) {
            throw new IllegalArgumentException("No eligible worker node found");
        }
        return eligibleNodeIds;
    }

    /**
     * Get worker node of specific model without filtering eligible node.
     *
     * @param modelId model id
     * @return list of worker node ids
     */
    public String[] getWorkerNodes(String modelId) {
        return getWorkerNodes(modelId, false);
    }

    /**
     * Get predictable instance with model id.
     *
     * @param modelId model id
     * @return predictable instance
     */
    public Predictable getPredictor(String modelId) {
        return modelCacheHelper.getPredictor(modelId);
    }

    /**
     * Get all model ids in cache, both local model id and remote model in routing table.
     *
     * @return array of model ids
     */
    public String[] getAllModelIds() {
        return modelCacheHelper.getAllModels();
    }

    /**
     * Get all local model ids.
     *
     * @return array of local deployed models
     */
    public String[] getLocalDeployedModels() {
        return modelCacheHelper.getDeployedModels();
    }

    /**
     * Sync model routing table.
     *
     * @param modelWorkerNodes model worker nodes
     */
    public synchronized void syncModelWorkerNodes(Map<String, Set<String>> modelWorkerNodes) {
        modelCacheHelper.syncWorkerNodes(modelWorkerNodes);
    }

    /**
     * Clear all model worker nodes from cache.
     */
    public void clearRoutingTable() {
        modelCacheHelper.clearWorkerNodes();
    }

    public MLModelProfile getModelProfile(String modelId) {
        return modelCacheHelper.getModelProfile(modelId);
    }

    public <T> T trackPredictDuration(String modelId, Supplier<T> supplier) {
        long start = System.nanoTime();
        T t = supplier.get();
        long end = System.nanoTime();
        double durationInMs = (end - start) / 1e6;
        modelCacheHelper.addModelInferenceDuration(modelId, durationInMs);
        return t;
    }

    public FunctionName getModelFunctionName(String modelId) {
        return modelCacheHelper.getFunctionName(modelId);
    }

    public Optional<FunctionName> getOptionalModelFunctionName(String modelId) {
        return modelCacheHelper.getOptionalFunctionName(modelId);
    }

    public boolean isModelRunningOnNode(String modelId) {
        return modelCacheHelper.isModelRunningOnNode(modelId);
    }

}
