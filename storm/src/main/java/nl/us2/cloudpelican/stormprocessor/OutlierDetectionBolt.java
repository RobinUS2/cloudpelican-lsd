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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import nl.us2.timeseriesoutlierdetection.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.*;

/**
 *
 * @author robin
 */
public class OutlierDetectionBolt extends BaseRichBolt {

    OutputCollector _collector;
    private Settings settings;
    private HashMap<String, Long> liveFilters;
    private HashMap<String, Long> filterMaxTsAnalayzed;
    private JsonParser jsonParser;
    private List<ITimeserieAnalyzer> analyzers;
    private long startTime;
    private static final int MIN_UPTIME = 60; // Seconds before this class starts outlier detection
    private static final int NUM_CORES = 4;

    private static final Logger LOG = LoggerFactory.getLogger(OutlierDetectionBolt.class);

    public OutlierDetectionBolt(Settings settings) {
        super();
        this.settings = settings;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        liveFilters = new HashMap<String, Long>();
        jsonParser = new JsonParser();
        filterMaxTsAnalayzed = new HashMap<String, Long>();

        // Active analyzers
        analyzers = new ArrayList<ITimeserieAnalyzer>();
        analyzers.add(new NoopTimeserieAnalyzer());
        analyzers.add(new NormalDistributionTimeserieAnalyzer());
        analyzers.add(new LogNormalDistributionTimeserieAnalyzer());
        analyzers.add(new SimpleRegressionTimeserieAnalyzer());
        analyzers.add(new MovingAverageTimeserieAnalyzer());
        analyzers.add(new PolynomialRegressionTimeserieAnalyzer());
        analyzers.add(new IntervalInterceptorTimeserieAnalyzer());
        analyzers.add(new RandomWalkRegressionTimeserieAnalyzer());
        analyzers.add(new OneClassSVMTimeserieAnalyzer());
        analyzers.add(new TimeBucketSimpleRegressionTimeserieAnalyzer());
        analyzers.add(new MultipleLinearRegressionTimeserieAnalyzer());
        analyzers.add(new SimpleExponentialSmoothingTimeserieAnalyzer());

        // Start time
        startTime = now();
    }

    protected long now() {
        return new Date().getTime() / 1000L;
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
        // Only process after a while to prevent issues after a cold start (classifiers still learning, bumps in traffic, etc)
        long uptime = now() - startTime;
        if (uptime < MIN_UPTIME) {
            LOG.info("Not running, uptime is " + uptime + " did not reach threshold of " + MIN_UPTIME + " second(s)");
            return;
        }

        // Clean up list
        ArrayList<String> toRemove = new ArrayList<String>();
        long maxAge = new Date().getTime() - (1000 * 10 * 60); // Considered stale after X minutes
        for (Map.Entry<String, Long> kv : liveFilters.entrySet()) {
            if (kv.getValue() < maxAge) {
                toRemove.add(kv.getKey());
            }
        }
        for (String k : toRemove) {
            liveFilters.remove(k);
            LOG.info("Removed stale filter " + k);
        }

        // Run outlier checks
        for (String filterId : liveFilters.keySet()) {
            try {
                _checkOutlier(filterId);
            } catch (Exception e) {
                LOG.error("Failed to check outliers of filter " + filterId, e);
            }
        }
    }

    protected void _checkOutlier(String filterId) throws Exception {
        // Data
        HttpGet getStats = new HttpGet(settings.get("supervisor_host") + "filter/" + filterId + "/stats");
        String token = new String(Base64.encodeBase64((settings.get("supervisor_username") + ":" + settings.get("supervisor_password")).getBytes()));
        getStats.addHeader("Authorization", "Basic " + token);
        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse resp = client.execute(getStats);
        String body = EntityUtils.toString(resp.getEntity());
        JsonObject stats = jsonParser.parse(body).getAsJsonObject().get("stats").getAsJsonObject();

        // Detect outliers
        MutableDataLoader dl = new MutableDataLoader("fkh-" + filterId);
        long now = new Date().getTime();
        long unixTs = now / 1000L;
        int timeResolution = 300; // in seconds
        long unixTsBucket = unixTs - (unixTs % timeResolution);
        long minTs = unixTsBucket - 24*3600; // x hours in past
        int skipLastSeconds = 1*timeResolution;
        long maxTs = unixTsBucket - skipLastSeconds; // Skip last X seconds
        long dataMaxTs = Long.MIN_VALUE;
        int dataPointCount = 0;
        for (Map.Entry<String, JsonElement> kv : stats.entrySet()) {
            String serieName = kv.getKey().equals("1") ? "regular" : "error";
            for (Map.Entry<String, JsonElement> tskv : kv.getValue().getAsJsonObject().entrySet()) {
                Long ts = Long.parseLong(tskv.getKey());
                if (ts < minTs || ts >= maxTs) {
                    continue;
                }
                if (ts > dataMaxTs) {
                    dataMaxTs = ts;
                }
                dl.addData(serieName, tskv.getKey(), tskv.getValue().getAsString());
                dataPointCount++;
            }
        }

        // Do we have at least 10 datapoints?
        if (dataPointCount < 10) {
            return;
        }

        // Check dataMaxTs against local test to reduce overhead
        long lastAnalyzed = filterMaxTsAnalayzed.getOrDefault(filterId, 0L);
        if (dataMaxTs <= lastAnalyzed) {
            // Do nothing
            return;
        }
        filterMaxTsAnalayzed.put(filterId, dataMaxTs);

        // Analyze
        dl.setDesiredTimeResolution(timeResolution);
        dl.setForecastPeriods(1);
        dl.load();
        dl.analyze(analyzers, NUM_CORES);
        List<ValidatedTimeserieOutlier> outliers = dl.validate();
        for (ValidatedTimeserieOutlier outlier : outliers) {
            LOG.info("Filter "  + filterId + " outlier at " + outlier.getTs() + " (" + new Date(outlier.getTs() * 1000L).toString() + ") score " + outlier.getScore());
            _collector.emit("outliers", new Values(filterId, outlier.getTs(), outlier.getScore(), outlier.getDetails().toString()));
        }
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 60;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }


    public void executeTuple(Tuple tuple) {
        String filterId = tuple.getStringByField("filter_id");

        liveFilters.put(filterId, new Date().getTime());

        // No ack, is handled in outer
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("outliers", new Fields("filter_id", "timestamp", "score", "json_details"));
    }
}