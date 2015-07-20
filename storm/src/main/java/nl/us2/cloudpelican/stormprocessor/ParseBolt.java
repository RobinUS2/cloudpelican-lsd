package nl.us2.cloudpelican.stormprocessor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by robin on 20/07/15.
 */
public class ParseBolt  extends BaseRichBolt {

    OutputCollector _collector;
    private Settings settings;
    private LogTypeSniffer lts;
    private SimpleDateFormat iso8601sdf;

    private static final Logger LOG = LoggerFactory.getLogger(ParseBolt.class);

    public ParseBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        lts = new LogTypeSniffer();
        iso8601sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    }

    public void execute(Tuple tuple) {
        executeTuple(tuple);
        _collector.ack(tuple);
    }

    public void executeTuple(Tuple tuple) {
        try {
            String msg = tuple.getString(0);
            if (msg == null) {
                return;
            }
            msg = msg.trim();
            if (msg.isEmpty()) {
                return;
            }

            // Sniff type
            LogSniffResult res = lts.sniff(msg);

            // Parse date
            Date ts = null;
            if (res.getTypes().contains(LogTypes.RSYSLOG)) {
                try {
                    ts = iso8601sdf.parse(res.getDateStr());
                } catch (ParseException pe) {
                    LOG.error("Failed to parse date", pe);
                }
            }
            if (ts == null) {
                ts = new Date(); // Fallback to now()
            }
            LOG.info(res.getDateStr() + " " + ts.toString() + " " + ts.getTime());

            // @todo Finish
        } catch (Exception e) {
            LOG.error("Unexpected error in executeTuple", e);
        }

        // No ack, is handled in outer
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("rollup_stats", new Fields("filter_id", "metric", "time_bucket", "increment"));
    }
}