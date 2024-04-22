/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.ml.common.transport;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.opensearch.action.ActionRequest;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractGetRequest extends ActionRequest {
    @Setter
    @Getter
    private String tenantId;

    public AbstractGetRequest(StreamInput in) throws IOException {
        super(in);
    }
}
