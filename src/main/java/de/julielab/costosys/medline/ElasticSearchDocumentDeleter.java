package de.julielab.costosys.medline;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ElasticSearchDocumentDeleter implements IDocumentDeleter {
    public static final String TO_DELETE_QUEUE = "elasticSearchDocumentDeletionQueue.lst";
    private static final String CONFKEY_CLUSTER = "configuration.clustername";
    private static final String CONFKEY_HOST = "configuration.host";
    private static final String CONFKEY_PORT = "configuration.port";
    private static final String CONFKEY_INDEX = "configuration.index";
    private static final String CONFKEY_TYPE = "configuration.type";
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchDocumentDeleter.class);
    private TransportClient client;

    private HierarchicalConfiguration<ImmutableNode> deletionConfiguration;

    @Override
    public void configure(HierarchicalConfiguration<ImmutableNode> deletionConfiguration) throws MedlineDocumentDeletionException {
        this.deletionConfiguration = deletionConfiguration;
        try {
            String clusterName = deletionConfiguration.getString(CONFKEY_CLUSTER);
            String host = deletionConfiguration.getString(CONFKEY_HOST);
            int port = deletionConfiguration.getInt(CONFKEY_PORT);
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            throw new MedlineDocumentDeletionException(e);
        }
    }

    @Override
    public void deleteDocuments(List<String> docIds) {
        try {
            String index = deletionConfiguration.getString(CONFKEY_INDEX);
            String type = deletionConfiguration.getString(CONFKEY_TYPE);
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (String id : docIds)
                bulkRequest.add(client.prepareDelete(index, type, id));
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                for (int i = 0; i < bulkResponse.getItems().length; i++) {
                    BulkItemResponse response = bulkResponse.getItems()[i];
                    if (response.isFailed())
                        log.error("Delete fail message: {}", response.getFailureMessage());
                }
            } else {
                log.info("Successfully deleted {} documents from ElasticSearch.", docIds.size());
            }
        } catch (Exception e) {
            log.error(
                    "Exception occurred while trying to delete documents from ElasticSearch. Document IDs that should have been deleted are stored in file {}.",
                    TO_DELETE_QUEUE);
            try {
                FileUtils.writeLines(new File(TO_DELETE_QUEUE), "UTF-8", docIds, "\n", true);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    @Override
    public Set<String> getNames() {
        return new HashSet<>(Arrays.asList("elasticsearch", getClass().getCanonicalName()));
    }

}
