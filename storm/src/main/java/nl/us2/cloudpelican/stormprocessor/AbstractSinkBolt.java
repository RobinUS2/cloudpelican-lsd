package nl.us2.cloudpelican.stormprocessor;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by robin on 30/06/15.
 */
public class AbstractSinkBolt extends BaseRichBolt {

    protected HashMap<String, ArrayList<String>> resultAggregator;
    protected OutputCollector _collector;
    protected String sinkId;
    protected Settings settings;
    protected int batchSize;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSinkBolt.class);

    public AbstractSinkBolt(String sinkId, Settings settings) {
        this.sinkId = sinkId;
        this.settings = settings;
        this.batchSize = 500;
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

    protected String _sinkVarPrefix(String k) {
        return "sinks." + sinkId + "." + k;
    }

    public String getSinkVar(String k) {
        return getSinkVarOrDefault(k, null);
    }

    public String getSinkVarOrDefault(String k, String d) {
        String pk = _sinkVarPrefix(k);
        String val = settings.getOrDefault(pk, d);
        LOG.info("Sink var: " + pk + "=" + val);
        return val;
    }

    public boolean isValid() {
        return false;
    }

    public void executeTick() {
        _flush();
    }

    protected void _flush() {
        throw new NotImplementedException();
    }

    public void executeTuple(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");
        String msg = tuple.getStringByField("msg");

        // Append in-memory
        if (!resultAggregator.containsKey(filterId)) {
            resultAggregator.put(filterId, new ArrayList<String>());
        }
        resultAggregator.get(filterId).add(msg);

        // Flush auto
        if (resultAggregator.get(filterId).size() >= batchSize) {
            _flush();
        }

        // No ack, is handled in outer
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        resultAggregator = new HashMap<String, ArrayList<String>>();
        prepareSink(conf, context, collector);
    }

    public void prepareSink(Map conf, TopologyContext context, OutputCollector collector) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
