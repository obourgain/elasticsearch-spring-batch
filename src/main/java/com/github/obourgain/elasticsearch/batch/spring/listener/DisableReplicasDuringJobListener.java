package com.github.obourgain.elasticsearch.batch.spring.listener;

import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import com.github.obourgain.elasticsearch.batch.util.ElasticsearchBatchOperationsSync;

/**
 * Disable replicas of target indices for the duration of the batch.
 * Having no replicas will speed up indexing operation and save CPU power as there is no need to push documents
 * from primary to replicas and perform the indexing there.
 * <p/>
 * After the job completion, the replica count will be reset to its previous value for every index, and Elasticsearch
 * will take care of duplicating the shards very efficiently by simple file transfer.
 * <p/>
 * Use this class only if you can afford to lose a part of the index during the job, e.g. if it is a cold index creation that can be easily relaunched.
 */
public class DisableReplicasDuringJobListener implements JobExecutionListener {

    private final ElasticsearchBatchOperationsSync operations;

    /**
     * The name of the property in the {@link ExecutionContext} that will map to String[] containing the
     * indices to perform the operation on.
     * <p/>
     * Defaults to DISABLE_REPLICAS_INDICES.
     */
    private String indicesContextProperty = "DISABLE_REPLICAS_INDICES";

    /**
     * The name of the property in the {@link ExecutionContext} that will contain the initial replica count values
     * to reset them at the end of the job.
     * The default value should be fine except if there are several instances of {@link DisableReplicasDuringJobListener} attached to
     * a single job (which have no pro compared to having only one).
     */
    private String initialReplicasContextProperty = "INITIAL_REPLICAS_COUNT";

    public DisableReplicasDuringJobListener(Client client) {
        this.operations = new ElasticsearchBatchOperationsSync(client);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String[] indices = (String[]) jobExecution.getExecutionContext().get(indicesContextProperty);
        if(indices == null) {
            throw new RuntimeException("No indices have been specified in the execution context to disable replicas, please configure the DisableReplicasDuringJobListener properly");
        }

        Map<String, Integer> initialReplicas = new HashMap<>();
        for (String index : indices) {
            int replicas = operations.getReplicas(index);
            initialReplicas.put(index, replicas);
        }
        jobExecution.getExecutionContext().put(initialReplicasContextProperty, initialReplicas);
        operations.disableReplicas(indices);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Map<String, Integer> initialReplicas = (Map<String, Integer>) jobExecution.getExecutionContext().get(initialReplicasContextProperty);
        for (Map.Entry<String, Integer> entry : initialReplicas.entrySet()) {
            operations.setReplicas(entry.getValue(), entry.getKey());
        }
    }

    public void setIndicesContextProperty(String indicesContextProperty) {
        this.indicesContextProperty = indicesContextProperty;
    }

    public void setInitialReplicasContextProperty(String initialReplicasContextProperty) {
        this.initialReplicasContextProperty = initialReplicasContextProperty;
    }
}
