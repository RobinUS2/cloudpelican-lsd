package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;
//import backtype.storm.tuple.Values;
//import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author robin
 */
public class MatchBolt extends BaseRichBolt {

    OutputCollector _collector;
    Pattern pattern;
    HashMap<String, Filter> filters;

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public MatchBolt(String regex) {
        super();
        if (regex != null) {
            LOG.info("Compiling local regex " + regex);
            pattern = Pattern.compile(regex);
        }
        filters = null;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
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
        loadFilters();
    }

    protected void loadFilters() {
        if (filters == null) {
            filters = new HashMap<String, Filter>();
        }
        // @Todo
    }

    protected HashMap<String, Filter> getFilters() {
        if (filters == null) {
            loadFilters();
        }
        return filters;
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 1;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }


    public void executeTuple(Tuple tuple) {
        String msg = tuple.getString(0).trim();
        // @todo check filters and output to next bolt
        getFilters();
        if (pattern != null) {
            Matcher m = pattern.matcher(msg);
            boolean b = m.find();
            if (b) {
                System.out.println(msg);
            }
        }
        //_collector.emit(DEFAULT_STREAM_ID, new Values(customerId, uuid, platform, interactionType, interactionValue, interactionUserDefinedSubtype, campaignId, creativeId, pixelId, eventType));
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("flxone_customer_id", "uuid", "platform", "interaction_type", "interaction_value", "interaction_user_defined_subtype", "campaign_id", "creative_id", "pixel_id", "event_type"));
    }
}