/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.ml.breaker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import java.io.File;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_DISK_FREE_SPACE_MIN_VALUE;

public class DiskCircuitBreakerTests {
    @Mock
    ClusterService clusterService;

    @Mock
    File file;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, new HashSet<>(List.of(
            ML_COMMONS_DISK_FREE_SPACE_MIN_VALUE))));
    }

    @Test
    public void test_isOpen_whenDiskFreeSpaceIsHigherThanMinValue_breakerIsNotOpen() {
        CircuitBreaker breaker = new DiskCircuitBreaker(Settings.builder().put(ML_COMMONS_DISK_FREE_SPACE_MIN_VALUE.getKey(), 5).build(), clusterService, file);
        when(file.getFreeSpace()).thenReturn(5 * 1024 * 1024 * 1024L);
        Assert.assertFalse(breaker.isOpen());
    }

    @Test
    public void test_isOpen_whenDiskFreeSpaceIsLessThanMinValue_breakerIsOpen() {
        CircuitBreaker breaker = new DiskCircuitBreaker(Settings.builder().put(ML_COMMONS_DISK_FREE_SPACE_MIN_VALUE.getKey(), 5).build(), clusterService, file);
        when(file.getFreeSpace()).thenReturn(4 * 1024 * 1024 * 1024L);
        Assert.assertTrue(breaker.isOpen());
    }

    @Test
    public void test_isOpen_whenDiskFreeSpaceConfiguredToZero_breakerIsNotOpen() {
        CircuitBreaker breaker = new DiskCircuitBreaker(Settings.builder().put(ML_COMMONS_DISK_FREE_SPACE_MIN_VALUE.getKey(), 5).build(), clusterService, file);
        when(file.getFreeSpace()).thenReturn((long)(Math.random() * 1024 * 1024 * 1024 * 1024L));
        Assert.assertFalse(breaker.isOpen());
    }

    @Test
    public void test_getName() {
        CircuitBreaker breaker = new DiskCircuitBreaker(Settings.EMPTY, clusterService, file);
        Assert.assertEquals("Disk Circuit Breaker", breaker.getName());
    }
}
