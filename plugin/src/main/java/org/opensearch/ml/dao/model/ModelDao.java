/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.ml.dao.model;

import org.opensearch.ml.common.MLModel;

import java.util.Optional;

public interface ModelDao {

    String createModel(MLModel mlModel);

    Optional<MLModel> getModel(String modelId, boolean isReturnContent);
}
