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
import java.text.SimpleDateFormat;
import java.util.*;

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
    private HashMap<String, Boolean> preparedTablesCache;

    private static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/bigquery";
    private static final int TABLE_STRUCTURE_VERSION = 1;

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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            try {
                // Prepare target table
                Date now = new Date();
                String date = sdf.format(now);
                String targetTable = kv.getKey() + "_results_" + date + " _v" + TABLE_STRUCTURE_VERSION;
                targetTable = targetTable.replace('-', '_');
                prepareTable(targetTable);

                // Log lines for debug
                LOG.info(kv.getValue().size() + " lines pending for " + kv.getKey());

                // Write to Google
                ArrayList<TableDataInsertAllRequest.Rows> rows = new ArrayList<TableDataInsertAllRequest.Rows>();
                for (String line : kv.getValue()) {
                    // Record
                    Map<String, Object> rowData = new HashMap<String, Object>();
                    rowData.put("_raw", line);
                    TableDataInsertAllRequest.Rows row = new TableDataInsertAllRequest.Rows().setJson(rowData);
                    rows.add(row);
                }

                // Execute
                TableDataInsertAllRequest ir = new TableDataInsertAllRequest().setRows(rows);
                TableDataInsertAllResponse response = bigquery.tabledata().insertAll(projectId, datasetId, targetTable, ir).execute();
                List<TableDataInsertAllResponse.InsertErrors> errors = response.getInsertErrors();
                if (errors != null) {
                    LOG.error(errors.size() + " error(s) while writing " + kv.getKey() + " to BigQuery");
                }
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
        schemaEntryMsg.setName("msg");
        schemaEntryMsg.setType("STRING");
        tableFieldSchema.add(schemaEntryMsg);

        TableFieldSchema schemaEntryMsgType = new TableFieldSchema();
        schemaEntryMsgType.setName("msg_type");
        schemaEntryMsgType.setType("INTEGER");
        tableFieldSchema.add(schemaEntryMsgType);

        TableFieldSchema schemaEntryLabel = new TableFieldSchema();
        schemaEntryLabel.setName("label");
        schemaEntryLabel.setType("STRING");
        tableFieldSchema.add(schemaEntryMsg);

        TableFieldSchema schemaEntryHost = new TableFieldSchema();
        schemaEntryHost.setName("host");
        schemaEntryHost.setType("STRING");
        tableFieldSchema.add(schemaEntryHost);

        TableFieldSchema schemaEntryTime = new TableFieldSchema();
        schemaEntryTime.setName("ts");
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