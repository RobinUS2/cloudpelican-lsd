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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 * @author robin
 */
public class SupervisorResultWriterBolt extends BaseRichBolt {

    OutputCollector _collector;
    HashMap<String, ArrayList<String>> resultAggregator;
    public Settings settings;
    private static final int BATCH_SIZE = 2000;
    private Executor executor;

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorResultWriterBolt.class);

    public SupervisorResultWriterBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        resultAggregator = new HashMap<String, ArrayList<String>>();
        executor = Executors.newFixedThreadPool(2);
    }

    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            _collector.ack(tuple);
            executeTick();
        } else {
            executeTuple(tuple);
            _collector.ack(tuple);
        }
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
            // To string
            StringBuilder sb = new StringBuilder();
            for (String line : data) {
                sb.append(line).append("\n");
            }

            // Execute async
            String url = settings.get("supervisor_host") + "filter/" + filterId + "/result";
            executor.execute(new SupervisorResultWriterRunnable(this, url, sb.toString()));

            // Clear
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