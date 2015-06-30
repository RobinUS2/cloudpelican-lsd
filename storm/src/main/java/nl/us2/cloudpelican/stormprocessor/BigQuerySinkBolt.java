package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpPut;
import org.apache.storm.http.entity.StringEntity;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author robin
 */
public class BigQuerySinkBolt extends AbstractSinkBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkBolt.class);
    private String projectId;
    private String datasetId;
    private String serviceAccountId;
    private String pk12KeyBase64;
    private PrivateKey pk12;
    private GoogleCredential googleCredential;
    private HttpTransport httpTransport;
    private Bigquery bigquery;

    private static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/bigquery";

    private static JsonFactory JSON_FACTORY;

    public BigQuerySinkBolt(String sinkId, Settings settings) {
        super(sinkId, settings);
    }

    public boolean isValid() {
        projectId = getSinkVar("project_id").trim();
        datasetId = getSinkVar("dataset_id").trim();
        serviceAccountId = getSinkVar("service_account_id").trim();
        pk12KeyBase64 = getSinkVar("pk12base64").trim();

        // Init key
        try {
            byte[] keyBytes = Base64.decodeBase64(pk12KeyBase64);
            ByteArrayInputStream bis = new ByteArrayInputStream(keyBytes);
            pk12 = SecurityUtils.loadPrivateKeyFromKeyStore(SecurityUtils.getPkcs12KeyStore(), bis, "notasecret", "privatekey", "notasecret");
            LOG.info("Loaded PK12 key");
        } catch (Exception e) {
            LOG.error("Failed to load private key", e);
            return false;
        }

        return !projectId.isEmpty() && !datasetId.isEmpty() && !serviceAccountId.isEmpty();
    }

    public void prepareSink(Map conf, TopologyContext context, OutputCollector collector) {
        isValid(); // Call isvalid to load key

        // Transport
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Exception e) {
            LOG.error("Failed to init transport", e);
            System.exit(1);
        }

        // JSON
        JSON_FACTORY = JacksonFactory.getDefaultInstance();

        // Build a service account credential.
        googleCredential = new GoogleCredential.Builder().setTransport(httpTransport)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(serviceAccountId)
                .setServiceAccountScopes(Collections.singleton(STORAGE_SCOPE))
                .setServiceAccountPrivateKey(pk12)
                .build();

        // BigQuery
        bigquery = new Bigquery.Builder(httpTransport, JSON_FACTORY, googleCredential).setApplicationName(Main.class.getSimpleName()).build();

        // Test call
        boolean tableExists = false;
        try {
            bigquery.tables().get(projectId, datasetId, "results").executeUsingHead();
            tableExists = true;
        } catch (Exception e) {
            LOG.error("Failed to check table", e);
        }
        LOG.info("Table exists " + String.valueOf(tableExists));
    }

    protected void _flush() {
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            LOG.info(kv.getValue().size() + " lines for " + kv.getKey());
        }
        resultAggregator.clear();
    }


}