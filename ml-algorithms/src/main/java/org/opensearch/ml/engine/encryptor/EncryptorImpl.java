/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.engine.encryptor;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import lombok.extern.log4j.Log4j2;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.ml.common.exception.MLException;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.ml.common.CommonValue.MASTER_KEY;
import static org.opensearch.ml.common.CommonValue.ML_CONFIG_INDEX;

@Log4j2
public class EncryptorImpl implements Encryptor {

    private ClusterService clusterService;
    private Client client;
    private volatile String masterKey;

    public EncryptorImpl(ClusterService clusterService, Client client) {
        this.masterKey = null;
        this.clusterService = clusterService;
        this.client = client;
    }

    public EncryptorImpl(String masterKey) {
        this.masterKey = masterKey;
    }

    @Override
    public void setMasterKey(String masterKey) {
        this.masterKey = masterKey;
    }

    @Override
    public String getMasterKey() {
        return masterKey;
    }

    @Override
    public String encrypt(String plainText) {
        initMasterKey();
        final AwsCrypto crypto = AwsCrypto.builder()
                .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
                .build();

        JceMasterKey jceMasterKey
                = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom", "",
                "AES/GCM/NoPadding");

        final CryptoResult<byte[], JceMasterKey> encryptResult = crypto.encryptData(jceMasterKey,
                plainText.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(encryptResult.getResult());
    }

    @Override
    public String decrypt(String encryptedText) {
        initMasterKey();
        final AwsCrypto crypto = AwsCrypto.builder()
                .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
                .build();

        JceMasterKey jceMasterKey
                = JceMasterKey.getInstance(new SecretKeySpec(masterKey.getBytes(), "AES"), "Custom", "",
                "AES/GCM/NoPadding");

        final CryptoResult<byte[], JceMasterKey> decryptedResult
                = crypto.decryptData(jceMasterKey, Base64.getDecoder().decode(encryptedText));
        return new String(decryptedResult.getResult());
    }

    @Override
    public String generateMasterKey() {
        byte[] keyBytes = new byte[16];
        new SecureRandom().nextBytes(keyBytes);
        String base64Key = Base64.getEncoder().encodeToString(keyBytes);
        return base64Key;
    }

    private void initMasterKey() {
        if (masterKey != null) {
            return;
        }
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);
        if (clusterService.state().metadata().hasIndex(ML_CONFIG_INDEX)) {
            GetRequest getRequest = new GetRequest(ML_CONFIG_INDEX).id(MASTER_KEY);
            client.get(getRequest, new LatchedActionListener(ActionListener.<GetResponse>wrap(r -> {
                if (r.isExists()) {
                    String masterKey = (String) r.getSourceAsMap().get(MASTER_KEY);
                    setMasterKey(masterKey);
                } else {
                    exceptionRef.set(new ResourceNotFoundException("ML encryption master key not initialized yet"));
                }
            }, e -> {
                log.error("Failed to get ML encryption master key", e);
                exceptionRef.set(e);
            }), latch));
        } else {
            exceptionRef.set(new ResourceNotFoundException("ML encryption master key not initialized yet"));
        }

        if (exceptionRef.get() != null) {
            log.debug("Failed to init master key", exceptionRef.get());
            if (exceptionRef.get() instanceof RuntimeException) {
                throw (RuntimeException) exceptionRef.get();
            } else {
                throw new MLException(exceptionRef.get());
            }
        }
    }
}
