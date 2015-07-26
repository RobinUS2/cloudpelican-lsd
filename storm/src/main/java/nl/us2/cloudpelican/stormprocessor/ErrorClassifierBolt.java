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

import java.util.*;

/**
 *
 * @author robin
 */
public class ErrorClassifierBolt extends BaseRichBolt {

    OutputCollector _collector;
    private Settings settings;
    private HashMap<String, Classifier<String, String>> classifiers;
    private HashMap<String, Long> samplesTrained;
    private String[] errorWords;
    private Random random;
    public static final String CLASSIFY_ERROR = "error";
    public static final String CLASSIFY_REGULAR = "regular";
    public static final long MIN_TRAIN_COUNT = 100L;

    private static final Logger LOG = LoggerFactory.getLogger(ErrorClassifierBolt.class);

    public ErrorClassifierBolt(Settings settings) {
        super();
        this.settings = settings;
        this.classifiers = new HashMap<String, Classifier<String, String>>();
        this.samplesTrained = new HashMap<String, Long>();
        this.errorWords = ("err;error;fail;failed;failure;timed out;exception;unexpected;not found;unauthorized;not authorized;missing;reject;rejected;drop;dropped;warn;warning;crit;critical;fatal;emerg;emergency;alert;404").split(";");
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        random  = new Random();
    }

    public void execute(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");
        String msg = tuple.getStringByField("msg");

        // Get classifier
        if (!classifiers.containsKey(filterId)) {
            classifiers.put(filterId, new BayesClassifier<String, String>());
            samplesTrained.put(filterId, 0L);
        }

        // Current train count
        long trainCount = samplesTrained.get(filterId);

        // Tokenize message
        List<String> msgTokens = Arrays.asList(msg.split("\\s+"));

        // Train classifier (all samples for the first 10K, afterwards 1 out of X
        if (trainCount < 10000L || random.nextInt(25) == 1) {
            // Word in blacklist?
            boolean errorWordMatch = false;
            String msgLower = msg.toLowerCase();
            for (String errorWord : errorWords) {
                if (msgLower.contains(errorWord)) {
                    errorWordMatch = true;
                    break;
                }
            }

            // Update classifiers
            if (errorWordMatch) {
                // Likely error
                LOG.debug("Train likely error: " + msg);
                classifiers.get(filterId).learn(CLASSIFY_ERROR, msgTokens);
            } else {
                // Likely regular message
                LOG.debug("Train likely regular: " + msg);
                classifiers.get(filterId).learn(CLASSIFY_REGULAR, msgTokens);
            }

            // Increment samples trained
            samplesTrained.put(filterId, trainCount + 1L);
        }

        // Match?
        if (trainCount >= MIN_TRAIN_COUNT && classifiers.get(filterId).classify(msgTokens).getCategory().equals(CLASSIFY_ERROR)) {
            LOG.debug("Classified as error: " + msg);
            _collector.emit("error_stats", new Values(filterId, tuple.getLongByField("ts"), MetricsEnum.ERRROR.getMask(), 1L)); // Counters
        }
        _collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("error_stats", new Fields("filter_id", "ts", "metric", "increment"));
    }
}