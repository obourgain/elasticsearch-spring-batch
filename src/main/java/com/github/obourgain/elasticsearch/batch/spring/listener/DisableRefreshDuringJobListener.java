package com.github.obourgain.elasticsearch.batch.spring.listener;

import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.client.Client;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import com.github.obourgain.elasticsearch.batch.util.ElasticsearchBatchOperationsSync;

/**
 * Disable refresh of target indices for the duration of the batch.
 * Disabling refresh can improve indexing performance by a few tens percents.
 * <p/>
 * After the job completion, the refresh interval will be reset to its previous value for every index.
 */
public class DisableRefreshDuringJobListener implements JobExecutionListener {

    private final ElasticsearchBatchOperationsSync operations;

    /**
     * The name of the property in the {@link ExecutionContext} that will map to String[] containing the
     * indices to perform the operation on.
     * <p/>
     * Defaults to REFRESH_INDICES.
     */
    private String indicesContextProperty = "REFRESH_INDICES";

    /**
     * The name of the property in the {@link ExecutionContext} that will contain the initial refresh interval values
     * to reset them at the end of the job.
     * The default value should be fine except if there are several instances of {@link DisableRefreshDuringJobListener} attached to
     * a single job.
     */
    private String initialRefreshIntervalContextProperty = "INITIAL_REFRESH_INTERVAL";

    public DisableRefreshDuringJobListener(Client client) {
        this.operations = new ElasticsearchBatchOperationsSync(client);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String[] indices = (String[]) jobExecution.getExecutionContext().get(indicesContextProperty);
        if(indices == null) {
            throw new RuntimeException("No indices have been specified in the execution context to disable refresh, please configure the DisableRefreshDuringJobListener properly");
        }

        Map<String, String> initialRefreshIntervals = new HashMap<>();
        for (String index : indices) {
            String refreshInterval = operations.getRefreshInterval(index);
            initialRefreshIntervals.put(index, refreshInterval);
        }
        jobExecution.getExecutionContext().put(initialRefreshIntervalContextProperty, initialRefreshIntervals);
        operations.disableRefresh(indices);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Map<String, String> initialRefreshIntervals = (Map<String, String>) jobExecution.getExecutionContext().get(initialRefreshIntervalContextProperty);
        for (Map.Entry<String, String> entry : initialRefreshIntervals.entrySet()) {
            operations.setRefreshInterval(entry.getValue(), entry.getKey());
        }
    }

    public void setIndicesContextProperty(String indicesContextProperty) {
        this.indicesContextProperty = indicesContextProperty;
    }

    public void setInitialRefreshIntervalContextProperty(String initialRefreshIntervalContextProperty) {
        this.initialRefreshIntervalContextProperty = initialRefreshIntervalContextProperty;
    }
}
