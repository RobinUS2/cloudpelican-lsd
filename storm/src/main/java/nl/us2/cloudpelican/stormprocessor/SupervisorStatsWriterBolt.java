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
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpPut;
import org.apache.storm.http.entity.ByteArrayEntity;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author robin
 */
public class SupervisorStatsWriterBolt extends BaseRichBolt {

    OutputCollector _collector;
    HashMap<String, SupervisorFilterStats> resultAggregator;
    private Settings settings;

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorStatsWriterBolt.class);

    public SupervisorStatsWriterBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        resultAggregator = new HashMap<String, SupervisorFilterStats>();
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
        HashMap<String, Long> pushMap = new HashMap<String, Long>();
        for (Map.Entry<String, SupervisorFilterStats> kv : resultAggregator.entrySet()) {
            String k = kv.getValue().toKey();
            long c = kv.getValue().getCount();
            LOG.debug(k + " = " + c);
            pushMap.put(k, c);
        }
        if (pushMap.size()  < 1) {
            return;
        }
        try {
            HttpClient client = HttpClientBuilder.create().build();

            String url = settings.get("supervisor_host") + "stats/filters";
//            LOG.debug(url);
            HttpPut put = new HttpPut(url);
            Gson gson = new Gson();
            String json = gson.toJson(pushMap);
//            LOG.debug(json);
//            StringEntity entity = new StringEntity(json);
//            put.setEntity(entity);

            // Gzip
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzos = null;
            try {
                gzos = new GZIPOutputStream(baos);
                gzos.write(json.getBytes("UTF-8"));
            } finally {
                if (gzos != null) try { gzos.close(); } catch (IOException ignore) {}
            }
            byte[] gzipBytes = baos.toByteArray();
            put.setEntity(new ByteArrayEntity(gzipBytes));
            put.setHeader("Content-Encoding", "gzip");

            // Token
            String token = new String(Base64.encodeBase64((settings.get("supervisor_username") + ":" + settings.get("supervisor_password")).getBytes()));
//            LOG.debug(token);
            put.setHeader("Authorization", "Basic " + token);
            HttpResponse resp = client.execute(put);
            int status = resp.getStatusLine().getStatusCode();
            if (status >= 400) {
                throw new Exception("Invalid status " + status);
            }
        } catch (Exception e) {
            LOG.error("Failed to write statistics to supervisor", e);
        }
        resultAggregator.clear();
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }


    public void executeTuple(Tuple tuple) {
        try {
            String filterId = tuple.getStringByField("filter_id");
            int metric = tuple.getIntegerByField("metric");
            long timeBucket = tuple.getLongByField("time_bucket");
            long increment = tuple.getLongByField("increment");

            long ts = timeBucket;
            long bucket = ts - (ts % 60); // Minutely buckets
            String k = SupervisorFilterStats.getKey(filterId, metric, increment);


            // Append in-memory
            if (!resultAggregator.containsKey(k)) {
                resultAggregator.put(k, new SupervisorFilterStats(filterId, metric, bucket));
            }
            resultAggregator.get(k).increment(increment);
        } catch (Exception e) {
            LOG.error("Unexpected error in executeTuple", e);
        }

        // No ack, is handled in outer
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}