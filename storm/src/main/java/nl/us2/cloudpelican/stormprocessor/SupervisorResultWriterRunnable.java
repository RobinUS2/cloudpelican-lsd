package nl.us2.cloudpelican.stormprocessor;

import org.apache.commons.codec.binary.Base64;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpPut;
import org.apache.storm.http.entity.ByteArrayEntity;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Created by robin on 16/07/15.
 */
public class SupervisorResultWriterRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorResultWriterRunnable.class);

    private final SupervisorResultWriterBolt parent;
    private final String data;
    private final String url;
    public SupervisorResultWriterRunnable(SupervisorResultWriterBolt parent, String url, String data) {
        this.parent = parent;
        this.url = url;
        this.data = data;
    }

    public void run() {
        try {
            HttpClient client = HttpClientBuilder.create().build();
            HttpPut put = new HttpPut(url);

            // Gzip
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzos = null;
            try {
                gzos = new GZIPOutputStream(baos);
                gzos.write(data.getBytes("UTF-8"));
            } finally {
                if (gzos != null) try { gzos.close(); } catch (IOException ignore) {}
            }
            byte[] gzipBytes = baos.toByteArray();
            put.setEntity(new ByteArrayEntity(gzipBytes));
            put.setHeader("Content-Encoding", "gzip");

            // Token
            String token = new String(Base64.encodeBase64((parent.settings.get("supervisor_username") + ":" + parent.settings.get("supervisor_password")).getBytes()));
            LOG.debug(token);
            put.setHeader("Authorization", "Basic " + token);

            // Execute
            HttpResponse resp = client.execute(put);
            int status = resp.getStatusLine().getStatusCode();
            if (status >= 400) {
                throw new Exception("Invalid status " + status);
            }
        } catch (Exception e) {
            LOG.error("Failed to write to SuperVisor", e);
        }
    }
}
