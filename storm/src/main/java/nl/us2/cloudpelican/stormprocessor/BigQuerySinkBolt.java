package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.PrivateKey;
import java.util.*;

/**
 *
 * @author robin
 */
public class BigQuerySinkBolt extends AbstractSinkBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkBolt.class);
    public String projectId;
    public String datasetId;
    private String serviceAccountId;
    private String pk12KeyBase64;
    private PrivateKey pk12;
    private GoogleCredential googleCredential;
    private HttpTransport httpTransport;
    public Bigquery bigquery;
    private HashMap<String, Boolean> preparedTablesCache;

    private static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/bigquery";
    public static final int TABLE_STRUCTURE_VERSION = 1;

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

        // Cache
        preparedTablesCache = new HashMap<String, Boolean>();
    }

    protected void _flush() {
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            try {
                // Write to Google format
                ArrayList<TableDataInsertAllRequest.Rows> rows = new ArrayList<TableDataInsertAllRequest.Rows>();
                for (String line : kv.getValue()) {
                    // Record
                    Map<String, Object> rowData = new HashMap<String, Object>();
                    rowData.put("_raw", line);
                    TableDataInsertAllRequest.Rows row = new TableDataInsertAllRequest.Rows().setJson(rowData);
                    rows.add(row);
                }

                // Execute async
                executor.execute(new BigQueryInsertRunnable(this, kv.getKey(), rows));
            } catch (Exception e) {
                LOG.error("Failed to write data of " + kv.getKey() + " to BigQuery", e);
            }
        }
        resultAggregator.clear();
    }

    protected void prepareTable(String name) {
        // From cache?
        if (preparedTablesCache.containsKey(name)) {
            return;
        }
        LOG.info("Preparing table " + name);

        // Check table
        boolean tableExists = false;
        try {
            bigquery.tables().get(projectId, datasetId, name).executeUsingHead();
            tableExists = true;
        } catch (Exception e) {
            LOG.error("Failed to check table", e);
        }

        // Does the table exist?
        if (tableExists) {
            LOG.info("Table exists " + String.valueOf(tableExists));
            preparedTablesCache.put(name, true);
            // Done
            return;
        }

        // Expiration
        Date now = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.HOUR, Integer.parseInt(getSinkVarOrDefault("data_retention_hours", "168"))); // x hours persistent
        long expirationTime = cal.getTimeInMillis();
        LOG.info("Expiration set to " + new Date(expirationTime).toString());

        // Table definition
        TableSchema schema = new TableSchema();
        List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
        TableFieldSchema schemaEntryRaw = new TableFieldSchema();
        schemaEntryRaw.setName("_raw");
        schemaEntryRaw.setType("STRING");
        tableFieldSchema.add(schemaEntryRaw);

        TableFieldSchema schemaEntryMsg = new TableFieldSchema();
        schemaEntryMsg.setName("message");
        schemaEntryMsg.setType("STRING");
        tableFieldSchema.add(schemaEntryMsg);

        TableFieldSchema schemaEntryMsgType = new TableFieldSchema();
        schemaEntryMsgType.setName("type");
        schemaEntryMsgType.setType("INTEGER");
        tableFieldSchema.add(schemaEntryMsgType);

        TableFieldSchema schemaEntryLabel = new TableFieldSchema();
        schemaEntryLabel.setName("label");
        schemaEntryLabel.setType("STRING");
        tableFieldSchema.add(schemaEntryLabel);

        TableFieldSchema schemaEntryHost = new TableFieldSchema();
        schemaEntryHost.setName("host");
        schemaEntryHost.setType("STRING");
        tableFieldSchema.add(schemaEntryHost);

        TableFieldSchema schemaEntryTime = new TableFieldSchema();
        schemaEntryTime.setName("timestamp");
        schemaEntryTime.setType("TIMESTAMP");
        tableFieldSchema.add(schemaEntryTime);

        schema.setFields(tableFieldSchema);

        // Table
        Table table = new Table();
        table.setSchema(schema);
        TableReference tableRef = new TableReference();
        tableRef.setDatasetId(datasetId);
        tableRef.setProjectId(projectId);
        tableRef.setTableId(name);
        table.setExpirationTime(expirationTime);
        table.setTableReference(tableRef);

        // Create table
        try {
            bigquery.tables().insert(projectId, datasetId, table).execute();
            LOG.info("Created table " + name);
            preparedTablesCache.put(name, true);
        } catch (Exception e) {
            LOG.error("Failed to create table", e);
        }
    }


}