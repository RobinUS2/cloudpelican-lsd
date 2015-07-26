package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author robin
 */
public class RollupStatsBolt extends BaseRichBolt {

    OutputCollector _collector;
    HashMap<String, SupervisorFilterStats> resultAggregator;
    private Settings settings;

    private static final Logger LOG = LoggerFactory.getLogger(RollupStatsBolt.class);

    public RollupStatsBolt(Settings settings) {
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
        if (resultAggregator.size()  < 1) {
            return;
        }

        // Emit tuples
        for (Map.Entry<String, SupervisorFilterStats> kv : resultAggregator.entrySet()) {
            _collector.emit("rollup_stats", new Values(kv.getValue().getFilterId(), kv.getValue().getMetric(), kv.getValue().getBucket(), kv.getValue().getCount()));
        }

        resultAggregator.clear();
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 1;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }


    public void executeTuple(Tuple tuple) {
        try {
            String filterId = tuple.getStringByField("filter_id");
            int metric = tuple.getIntegerByField("metric");
            long increment = tuple.getLongByField("increment");

            long ts = tuple.getLongByField("ts");
            long bucket = ts - (ts % 1000); // Secondly buckets (1000 ms)
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
        declarer.declareStream("rollup_stats", new Fields("filter_id", "metric", "time_bucket", "increment"));
    }
}