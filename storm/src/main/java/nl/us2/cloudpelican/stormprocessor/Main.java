package nl.us2.cloudpelican.stormprocessor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by robin on 07/06/15.
 */
public class Main {
    public static String KAFKA_SPOUT = "kafka_spout";
    public static String MATCH_BOLT = "match_bolt";
    public static String SUPERVISOR_RESULT_WRITER = "supervisor_result_writer";
    public static String ROLLUP_STATS = "rollup_stats";
    public static String SUPERVISOR_STATS_WRITER = "supervisor_stats_writer";
    public static String ERROR_CLASSIFIER_BOLT = "error_classifier";
    public static String OUTLIER_DETECTION = "outlier_detection";
    public static String OUTLIER_COLLECTOR = "outlier_collector";

    private static boolean isRunning = true;
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static final int GLOBAL_CONCURRENCY = 6;

    public static void main(String [] args) throws Exception
    {
        ArrayList<String> argList = new ArrayList<String>();
        for (String arg : args) {
            argList.add(arg);
        }

        // Config
        HashMap<String, String> argsMap = new HashMap<String, String>();
        for (String arg : argList) {
            String[] split = arg.split("=", 2);
            if (split.length == 2 && split[0].trim().length() > 0 && split[1].trim().length() > 0) {
                if (split[0].equals("-zookeeper")) {
                    argsMap.put("zookeeper_nodes", split[1]);
                } else if (split[0].equals("-grep")) {
                    argsMap.put("match_regex", split[1]);
                } else if (split[0].equals("-topic")) {
                    argsMap.put("kafka_topic", split[1]);
                } else if (split[0].equals("-supervisor-host")) {
                    argsMap.put("supervisor_host", split[1]);
                } else if (split[0].equals("-supervisor-username")) {
                    argsMap.put("supervisor_username", split[1]);
                } else if (split[0].equals("-supervisor-password")) {
                    argsMap.put("supervisor_password", split[1]);
                } else if (split[0].equals("-conf")) {
                    argsMap.put("conf_path", split[1]);
                } else if (split[0].startsWith("-")) {
                    // Default
                    argsMap.put(split[0].substring(1), split[1]);
                }
            }
        }

        // Default settings
        if (!argsMap.containsKey("kafka_consumer_id")) {
            argsMap.put("kafka_consumer_id", "cloudpelican_lsd_consumer");
        }

        // Settings object
        Settings settings = new Settings();
        JsonObject settingsData = new JsonObject();

        // Add light settings to json
        for (Map.Entry<String, String> kv : argsMap.entrySet()) {
            settingsData.addProperty(kv.getKey(), kv.getValue());
        }

        // Debug & load
        LOG.info(settingsData.toString());
        settings.load(settingsData);

        // Topology
        TopologyBuilder builder = new TopologyBuilder();

        // Time
        TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));

        // Read from kafka

        BrokerHosts hosts = new ZkHosts(settings.get("zookeeper_nodes"));
        SpoutConfig spoutConfig = new SpoutConfig(hosts, settings.get("kafka_topic"), "/" + settings.get("kafka_topic"), settings.get("kafka_consumer_id"));
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout(KAFKA_SPOUT, kafkaSpout, Integer.parseInt(settings.getOrDefault("kafka_partitions", "3")));

        // Match bolt
        builder.setBolt(MATCH_BOLT, new MatchBolt(settings), GLOBAL_CONCURRENCY * 6).shuffleGrouping(KAFKA_SPOUT); // No local to prevent hotspots

        // Error classifier bolt
        builder.setBolt(ERROR_CLASSIFIER_BOLT, new ErrorClassifierBolt(settings), GLOBAL_CONCURRENCY * 1).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor result writer bolt
        builder.setBolt(SUPERVISOR_RESULT_WRITER, new SupervisorResultWriterBolt(settings), GLOBAL_CONCURRENCY * 1).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor stats writer bolt
        builder.setBolt(ROLLUP_STATS, new RollupStatsBolt(settings), concurrency(1, 2)).fieldsGrouping(MATCH_BOLT, "match_stats", new Fields("filter_id")).fieldsGrouping(ERROR_CLASSIFIER_BOLT, "error_stats", new Fields("filter_id"));
        builder.setBolt(SUPERVISOR_STATS_WRITER, new SupervisorStatsWriterBolt(settings), concurrency(1, 4)).fieldsGrouping(ROLLUP_STATS, "rollup_stats", new Fields("filter_id"));

        // Outlier detection bolts (sharded by filter ID)
        builder.setBolt(OUTLIER_DETECTION, new OutlierDetectionBolt(settings), GLOBAL_CONCURRENCY * 2).fieldsGrouping(MATCH_BOLT, "dispatch_outlier_checks", new Fields("filter_id"));
        builder.setBolt(OUTLIER_COLLECTOR, new OutlierCollectorBolt(settings), concurrency(1, 10)).shuffleGrouping(OUTLIER_DETECTION, "outliers");

        // Sink
        if (settings.get("sinks") != null) {
            String[] sinkIds = settings.get("sinks").split(",");
            for (String sinkId : sinkIds) {
                // Type
                String sinkType = settings.get("sinks." + sinkId + ".type");
                AbstractSinkBolt sinkBolt = null;
                // @todo Sink factory if we have multiple types
                if (sinkType == null) {
                    throw new Exception("Sink '" + sinkId + "' invalid");
                } else if (sinkType.equalsIgnoreCase("bigquery")) {
                    // Google BigQuery sink
                    sinkBolt = new BigQuerySinkBolt(sinkId, settings);
                }  else {
                    throw new Exception("Sink type '" + sinkType + "' not supported");
                }

                // Add to topology
                if (sinkBolt != null) {
                    String sinkName = "sink_" + sinkType + "_" + sinkId;
                    LOG.info("Setting up sink '" + sinkName + "'");
                    if (!sinkBolt.isValid()) {
                        LOG.error("Sink '" + sinkName + "' not valid");
                    }
                    builder.setBolt(sinkName, sinkBolt, GLOBAL_CONCURRENCY * 2).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));
                }
            }
        }

        // Debug on for testing
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(120); // Default is 30 seconds, which might be too short under peak load spikes, or when we run the outlier detection
        String topologyName = settings.getOrDefault("topology_name", "cloudpelican_stormprocessor");
        if (argList.contains("-submit")) {
            conf.setNumWorkers(GLOBAL_CONCURRENCY);
            conf.setNumAckers(concurrency(2, 10));
            conf.setMaxSpoutPending(GLOBAL_CONCURRENCY * 1000);
            conf.setStatsSampleRate(1.0); // Disable in production
            StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());

            // Keep running until interrupt
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run( ){
                    LOG.info("Shutting down");
                    isRunning = false;
                }
            });
            while (isRunning) {
                Thread.sleep(100);
            }

            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }

    public static int concurrency(int min, int part) {
        return Math.max(Math.round(GLOBAL_CONCURRENCY / part), min);
    }
}
