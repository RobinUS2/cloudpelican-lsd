package nl.us2.cloudpelican.stormprocessor;

import backtype.storm.tuple.Fields;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import java.io.File;
import java.util.*;

/**
 * Created by robin on 07/06/15.
 */
public class Main {
    public static String KAFKA_SPOUT = "kafka_spout";
    public static String MATCH_BOLT = "match_bolt";
    public static String SUPERVISOR_RESULT_WRITER = "supervisor_result_writer";
    public static String SUPERVISOR_STATS_WRITER = "supervisor_stats_writer";
    public static String SUPERVISOR_ERROR_STATS_WRITER = "supervisor_error_stats_writer";
    public static String ERROR_CLASSIFIER_BOLT = "error_classifier";
    private static boolean isRunning = true;
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

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
            argsMap.put("kafka_consumer_id", "default_cloudpelican_lsd_consumer");
        }

        // Load full settings from file
        Settings settings = new Settings();
        JsonObject settingsData;
        if (argsMap.containsKey("conf_path")) {
            String json = FileUtils.readFileToString(new File(argsMap.get("conf_path")));
            JsonParser jp = new JsonParser();
            settingsData = jp.parse(json).getAsJsonObject();
        } else {
            // Default empty
            settingsData = new JsonObject();
        }

        // Add light settings to json
        for (Map.Entry<String, String> kv : argsMap.entrySet()) {
            settingsData.addProperty(kv.getKey(), kv.getValue());
        }

        // Debug & load
        LOG.info(settingsData.toString());
        settings.load(settingsData);

        // Topology
        TopologyBuilder builder = new TopologyBuilder();
        int globalConcurrency = 1;

        // Time
        TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));

        // Read from kafka

        BrokerHosts hosts = new ZkHosts(settings.get("zookeeper_nodes"));
        SpoutConfig spoutConfig = new SpoutConfig(hosts, settings.get("kafka_topic"), "/" + settings.get("kafka_topic"), settings.get("kafka_consumer_id"));
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout(KAFKA_SPOUT, kafkaSpout, 3);

        // Match bolt
        builder.setBolt(MATCH_BOLT, new MatchBolt(settings), globalConcurrency * 8).localOrShuffleGrouping(KAFKA_SPOUT);

        // Error classifier bolt
        builder.setBolt(ERROR_CLASSIFIER_BOLT, new ErrorClassifierBolt(settings), globalConcurrency * 8).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor result writer bolt
        builder.setBolt(SUPERVISOR_RESULT_WRITER, new SupervisorResultWriterBolt(settings), globalConcurrency * 4).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor stats writer bolt
        builder.setBolt(SUPERVISOR_STATS_WRITER, new SupervisorStatsWriterBolt(settings), globalConcurrency * 2).fieldsGrouping(MATCH_BOLT, "match_stats", new Fields("filter_id"));
        builder.setBolt(SUPERVISOR_ERROR_STATS_WRITER, new SupervisorStatsWriterBolt(settings), globalConcurrency * 2).fieldsGrouping(ERROR_CLASSIFIER_BOLT, "error_stats", new Fields("filter_id"));

        // Debug on for testing
        Config conf = new Config();
        conf.setDebug(false);
        String topologyName = settings.getOrDefault("topology_name", "cloudpelican_stormprocessor");
        if (argList.contains("-submit")) {
            conf.setNumWorkers(globalConcurrency);
            conf.setNumAckers(globalConcurrency);
            conf.setMaxSpoutPending(1000);
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
}
