package nl.us2.cloudpelican.bigqueryclient;

import java.io.ByteArrayInputStream;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.logging.Logger;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
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
        JsonObject settings = jp.parse(args[0]).getAsJsonObject();

        String projectId = settings.get("project_id").getAsString();
        String datasetId = settings.get("dataset_id").getAsString();
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
    }
}
