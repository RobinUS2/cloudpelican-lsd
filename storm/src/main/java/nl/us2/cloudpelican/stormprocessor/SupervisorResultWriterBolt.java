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
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpPut;
import org.apache.storm.http.entity.ByteArrayEntity;
import org.apache.storm.http.entity.StringEntity;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author robin
 */
public class SupervisorResultWriterBolt extends BaseRichBolt {

    OutputCollector _collector;
    HashMap<String, ArrayList<String>> resultAggregator;
    private Settings settings;
    private static final int BATCH_SIZE = 5000;

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorResultWriterBolt.class);

    public SupervisorResultWriterBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        resultAggregator = new HashMap<String, ArrayList<String>>();
    }

    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            executeTick();
        } else {
            executeTuple(tuple);
        }
        _collector.ack(tuple);
    }

    public void executeTick() {
        _flush();
    }

    protected void _flush() {
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            _flushFilter(kv.getKey(), kv.getValue());
        }
        resultAggregator.clear();
    }

    protected void _flushFilter(String filterId, ArrayList<String> data) {
        try {
            HttpClient client = HttpClientBuilder.create().build();

            String url = settings.get("supervisor_host") + "filter/" + filterId + "/result";
            LOG.debug(url);
            HttpPut put = new HttpPut(url);
            StringBuilder sb = new StringBuilder();
            for (String line : data) {
                sb.append(line).append("\n");
            }
            //StringEntity entity = new StringEntity(sb.toString());
            //put.setEntity(entity);

            // Gzip
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzos = null;
            try {
                gzos = new GZIPOutputStream(baos);
                gzos.write(sb.toString().getBytes("UTF-8"));
            } finally {
                if (gzos != null) try { gzos.close(); } catch (IOException ignore) {}
            }
            byte[] gzipBytes = baos.toByteArray();
            put.setEntity(new ByteArrayEntity(gzipBytes));
            put.setHeader("Content-Encoding", "gzip");

            // Token
            String token = new String(Base64.encodeBase64((settings.get("supervisor_username") + ":" + settings.get("supervisor_password")).getBytes()));
            LOG.debug(token);
            put.setHeader("Authorization", "Basic " + token);

            // Execute
            HttpResponse resp = client.execute(put);
            int status = resp.getStatusLine().getStatusCode();
            if (status >= 400) {
                throw new Exception("Invalid status " + status);
            }
            data.clear();
        } catch (Exception e) {
            LOG.error("Failed to write data to supervisor", e);
        }
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 1;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    public void executeTuple(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");
        String msg = tuple.getStringByField("msg");

        // Append in-memory
        if (!resultAggregator.containsKey(filterId)) {
            resultAggregator.put(filterId, new ArrayList<String>());
        }
        resultAggregator.get(filterId).add(msg);

        // Flush if we have a lot of messages in memory
        if (resultAggregator.get(filterId).size() > BATCH_SIZE) {
            _flushFilter(filterId, resultAggregator.get(filterId));
        }

        // No ack, is handled in outer
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}