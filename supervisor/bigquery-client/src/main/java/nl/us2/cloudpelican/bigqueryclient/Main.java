package nl.us2.cloudpelican.bigqueryclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Base64;

/**
 * Created by robin on 01/07/15.
 */
public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class.getName());
    private static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/bigquery";

    public static void main(String[] args) throws Exception {
        // Parse params
        JsonParser jp = new JsonParser();
        JsonObject settings = jp.parse(new String(Base64.decodeBase64(args[0].getBytes()))).getAsJsonObject();

        String projectId = settings.get("project_id").getAsString();
        String serviceAccountId = settings.get("service_account_id").getAsString();
        String pk12KeyBase64 = settings.get("pk12base64").getAsString();

        // Init key
        PrivateKey pk12;
        try {
            byte[] keyBytes = Base64.decodeBase64(pk12KeyBase64.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(keyBytes);
            pk12 = SecurityUtils.loadPrivateKeyFromKeyStore(SecurityUtils.getPkcs12KeyStore(), bis, "notasecret", "privatekey", "notasecret");
            LOG.info("Loaded PK12 key");
        } catch (Exception e) {
            LOG.info(e.getMessage());
            System.exit(1);
            e.printStackTrace();
            return;
        }

        // Transport
        HttpTransport httpTransport;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Exception e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
            System.exit(1);
            return;
        }

        // JSON
        JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

        // Build a service account credential.
        GoogleCredential googleCredential = new GoogleCredential.Builder().setTransport(httpTransport)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(serviceAccountId)
                .setServiceAccountScopes(Collections.singleton(STORAGE_SCOPE))
                .setServiceAccountPrivateKey(pk12)
                .build();

        // BigQuery
        Bigquery bigquery = new Bigquery.Builder(httpTransport, JSON_FACTORY, googleCredential).setApplicationName(Main.class.getSimpleName()).build();

        // Start a Query Job
        String querySql = settings.get("query").getAsString();
        JobReference jobId = startQuery(bigquery, projectId, querySql);

        // Poll for Query Results, return result output
        Job completedJob = checkQueryResults(bigquery, projectId, jobId);

        // Return and display the results of the Query Job
        displayQueryResults(bigquery, projectId, completedJob);
    }

    // [START start_query]
    /**
     * Creates a Query Job for a particular query on a dataset
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a String containing the project ID
     * @param querySql  the actual query string
     * @return a reference to the inserted query job
     * @throws IOException
     */
    public static JobReference startQuery(Bigquery bigquery, String projectId,
                                          String querySql) throws IOException {
        System.err.format("\nInserting Query Job: %s\n", querySql);

        Job job = new Job();
        JobConfiguration config = new JobConfiguration();
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        config.setQuery(queryConfig);

        job.setConfiguration(config);
        queryConfig.setQuery(querySql);

        Bigquery.Jobs.Insert insert = bigquery.jobs().insert(projectId, job);
        insert.setProjectId(projectId);
        JobReference jobId = insert.execute().getJobReference();

        System.err.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

        return jobId;
    }

    /**
     * Polls the status of a BigQuery job, returns Job reference if "Done"
     *
     * @param bigquery  an authorized BigQuery client
     * @param projectId a string containing the current project ID
     * @param jobId     a reference to an inserted query Job
     * @return a reference to the completed Job
     * @throws IOException
     * @throws InterruptedException
     */
    private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
            throws IOException, InterruptedException {
        // Variables to keep track of total query time
        long startTime = System.currentTimeMillis();
        long elapsedTime;

        while (true) {
            Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
            elapsedTime = System.currentTimeMillis() - startTime;
            System.err.format("Job status (%dms) %s: %s\n", elapsedTime,
                    jobId.getJobId(), pollJob.getStatus().getState());
            if (pollJob.getStatus().getState().equals("DONE")) {
                return pollJob;
            }
            // Pause execution for one second before polling job status again, to
            // reduce unnecessary calls to the BigQUery API and lower overall
            // application bandwidth.
            Thread.sleep(1000);
        }
    }
    // [END start_query]

    // [START display_result]
    /**
     * Makes an API call to the BigQuery API
     *
     * @param bigquery     an authorized BigQuery client
     * @param projectId    a string containing the current project ID
     * @param completedJob to the completed Job
     * @throws IOException
     */
    private static void displayQueryResults(Bigquery bigquery,
                                            String projectId, Job completedJob) throws IOException {
        GetQueryResultsResponse queryResult = bigquery.jobs()
                .getQueryResults(
                        projectId, completedJob
                                .getJobReference()
                                .getJobId()
                ).execute();
        List<TableRow> rows = queryResult.getRows();
        System.err.print("\nQuery Results:\n------------\n");
        for (TableRow row : rows) {
            for (TableCell field : row.getF()) {
                System.out.printf("%s\t", field.getV());
            }
            System.out.println();
        }
    }
    // [END display_result]
}
