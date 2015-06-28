package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.daslaboratorium.machinelearning.classifier.BayesClassifier;
import de.daslaboratorium.machinelearning.classifier.Classifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author robin
 */
public class ErrorClassifierBolt extends BaseRichBolt {

    OutputCollector _collector;
    private HashMap<String, String> settings;
    private HashMap<String, Classifier<String, String>> classifiers;
    private String[] errorWords;
    public static String CLASSIFY_ERROR = "error";
    public static String CLASSIFY_REGULAR = "regular";

    private static final Logger LOG = LoggerFactory.getLogger(ErrorClassifierBolt.class);

    public ErrorClassifierBolt(HashMap<String, String> settings) {
        super();
        this.settings = settings;
        this.classifiers = new HashMap<String, Classifier<String, String>>();
        this.errorWords = ("err;error;fail;failed;failure;timed out;exception;unexpected;not found;unauthorized;not authorized;missing;reject;rejected;drop;dropped;warn;warning;crit;critical;fatal;emerg;emergency;alert;404").split(";");
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");
        String msg = tuple.getStringByField("msg");
        String msgLower = msg.toLowerCase();

        // Get classifier
        if (!classifiers.containsKey(filterId)) {
            classifiers.put(filterId, new BayesClassifier<String, String>());
        }

        // Word in blacklist?
        List<String> msgTokens = Arrays.asList(msg.split("\\s+"));
        boolean errorWordMatch = false;
        for (String errorWord : errorWords) {
            if (msgLower.contains(errorWord)) {
                errorWordMatch = true;
                break;
            }
        }

        // Train classifier
        if (errorWordMatch) {
            // Likely error
            LOG.debug("Train likely error: " + msg);
            classifiers.get(filterId).learn(CLASSIFY_ERROR, msgTokens);
        } else {
            // Likely regular message
            LOG.debug("Train likely regular: " + msg);
            classifiers.get(filterId).learn(CLASSIFY_REGULAR, msgTokens);
        }

        // Match?
        if (classifiers.get(filterId).classify(msgTokens).getCategory().equals(CLASSIFY_ERROR)) {
            LOG.debug("Classified as error: " + msg);
            _collector.emit("error_stats", new Values(filterId, MetricsEnum.ERRROR.getMask(), 1L)); // Counters
        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("error_stats", new Fields("filter_id", "metric", "increment"));
    }
}