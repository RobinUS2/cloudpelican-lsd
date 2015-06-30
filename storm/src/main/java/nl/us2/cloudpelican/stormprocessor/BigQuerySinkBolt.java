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
    private String projectId;
    private String datasetId;
    private String serviceAccountId;
    private String pk12Key;

    public BigQuerySinkBolt(String sinkId, Settings settings) {
        super(sinkId, settings);
    }

    public boolean isValid() {
        projectId = getSinkVar("project_id").trim();
        datasetId = getSinkVar("dataset_id").trim();
        serviceAccountId = getSinkVar("service_account_id").trim();
        pk12Key = getSinkVar("pk12base64").trim();
        return !projectId.isEmpty() && !datasetId.isEmpty();
    }

    protected void _flush() {
        for (Map.Entry<String, ArrayList<String>> kv : resultAggregator.entrySet()) {
            LOG.info(kv.getValue().size() + " lines for " + kv.getKey());
            LOG.info(projectId);
        }
        resultAggregator.clear();
    }


}