package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
//import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
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

    public MatchBolt(String regex) {
        super();
        pattern = Pattern.compile(regex);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        String msg = tuple.getString(0).trim();

        Matcher m = pattern.matcher(msg);
        boolean b = m.find();
        if (b) {
            System.out.println(msg);
        }
        //_collector.emit(DEFAULT_STREAM_ID, new Values(customerId, uuid, platform, interactionType, interactionValue, interactionUserDefinedSubtype, campaignId, creativeId, pixelId, eventType));
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("flxone_customer_id", "uuid", "platform", "interaction_type", "interaction_value", "interaction_user_defined_subtype", "campaign_id", "creative_id", "pixel_id", "event_type"));
    }
}