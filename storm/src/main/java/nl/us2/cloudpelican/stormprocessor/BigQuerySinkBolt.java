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
import org.apache.storm.http.entity.StringEntity;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author robin
 */
public class BigQuerySinkBolt extends AbstractSinkBolt {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkBolt.class);

    public BigQuerySinkBolt(String sinkId, Settings settings) {
        super(sinkId, settings);
    }

    public boolean isValid() {
        // @todo validate
        LOG.info(getSinkVar("project_id"));
        return true;
    }

    protected void _flush() {
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            LOG.info(kv.getValue().size() + " lines for " + kv.getKey());
        }
        resultAggregator.clear();
    }


}