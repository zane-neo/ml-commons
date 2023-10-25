/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.connector;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.opensearch.ml.common.output.MLOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.opensearch.ml.common.connector.MLPostProcessFunction.OPENAI_EMBEDDING;

public class MLPostProcessFunctionTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void contains() {
        Assert.assertTrue(MLPostProcessFunction.contains(OPENAI_EMBEDDING));
        Assert.assertFalse(MLPostProcessFunction.contains("wrong value"));
    }

    @Test
    public void get() {
        Assert.assertNotNull(MLPostProcessFunction.get(OPENAI_EMBEDDING));
        Assert.assertNull(MLPostProcessFunction.get("wrong value"));
    }

    @Test
    public void test_getResponseFilter() {
        assert null != MLPostProcessFunction.getResponseFilter(OPENAI_EMBEDDING);
        assert null == MLPostProcessFunction.getResponseFilter("wrong value");
    }

    @Test
    public void test_buildListResultModelTensors() {
        Assert.assertNotNull(MLPostProcessFunction.buildListResultModelTensors());
        List<List<Float>> numbersList = new ArrayList<>();
        numbersList.add(Collections.singletonList(1.0f));
        Assert.assertNotNull(MLPostProcessFunction.buildListResultModelTensors().apply(numbersList));
    }

    @Test
    public void test_buildListResultModelTensors_exception() {
        exceptionRule.expect(IllegalArgumentException.class);
        MLPostProcessFunction.buildListResultModelTensors().apply(null);
        ArgumentCaptor<IllegalArgumentException> argumentCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        assertEquals("The list of embeddings is null when using the built-in post-processing function.", argumentCaptor.capture().getMessage());
    }

    @Test
    public void test_buildSingleResultModelTensors() {
        Assert.assertNotNull(MLPostProcessFunction.buildSingleResultModelTensor());
        List<Float> numbersList = Collections.singletonList(1.0f);
        Assert.assertNotNull(MLPostProcessFunction.buildSingleResultModelTensor().apply(numbersList));
    }

    @Test
    public void test_buildSingleResultModelTensors_exception() {
        exceptionRule.expect(IllegalArgumentException.class);
        ArgumentCaptor<IllegalArgumentException> argumentCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        MLPostProcessFunction.buildSingleResultModelTensor().apply(null);
        assertEquals("The embeddings is null when using the built-in post-processing function.", argumentCaptor.capture().getMessage());
    }
}
