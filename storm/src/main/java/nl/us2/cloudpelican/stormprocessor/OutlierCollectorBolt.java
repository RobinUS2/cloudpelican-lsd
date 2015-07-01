package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author robin
 */
public class OutlierCollectorBolt extends BaseRichBolt {

    OutputCollector _collector;
    private Settings settings;

    private static final Logger LOG = LoggerFactory.getLogger(OutlierCollectorBolt.class);

    public OutlierCollectorBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");
        long ts = tuple.getLongByField("timestamp");
        double score = tuple.getDoubleByField("score");
        String jsonDetails = tuple.getStringByField("json_details");
        LOG.info(filterId + " " + ts + " " + score + " " + jsonDetails);

        // Send to supervisor
        try {
            HttpClient client = HttpClientBuilder.create().build();

            String url = settings.get("supervisor_host") + "filter/" + filterId + "/outlier?timestamp=" + ts + "&score=" + score;
            HttpPost req = new HttpPost(url);
            req.setEntity(new StringEntity(jsonDetails));
            String token = new String(Base64.encodeBase64((settings.get("supervisor_username") + ":" + settings.get("supervisor_password")).getBytes()));
            req.setHeader("Authorization", "Basic " + token);
            HttpResponse resp = client.execute(req);
            int status = resp.getStatusLine().getStatusCode();
            if (status >= 400) {
                throw new Exception("Invalid status " + status);
            }
        } catch (Exception e) {
            LOG.error("Failed to write data to supervisor", e);
        }

        _collector.ack(tuple);
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}