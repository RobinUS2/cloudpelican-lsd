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
        HashMap<String, String> lightSettings = new HashMap<String, String>();
        for (String arg : argList) {
            String[] split = arg.split("=", 2);
            if (split.length == 2 && split[0].trim().length() > 0 && split[1].trim().length() > 0) {
                if (split[0].equals("-zookeeper")) {
                    lightSettings.put("zookeeper_nodes", split[1]);
                } else if (split[0].equals("-grep")) {
                    lightSettings.put("match_regex", split[1]);
                } else if (split[0].equals("-topic")) {
                    lightSettings.put("kafka_topic", split[1]);
                } else if (split[0].equals("-supervisor-host")) {
                    lightSettings.put("supervisor_host", split[1]);
                } else if (split[0].equals("-supervisor-username")) {
                    lightSettings.put("supervisor_username", split[1]);
                } else if (split[0].equals("-supervisor-password")) {
                    lightSettings.put("supervisor_password", split[1]);
                } else if (split[0].equals("-conf")) {
                    lightSettings.put("conf_path", split[1]);
                } else if (split[0].startsWith("-")) {
                    // Default
                    lightSettings.put(split[0].substring(1), split[1]);
                }
            }
        }

        // Default settings
        if (!lightSettings.containsKey("kafka_consumer_id")) {
            lightSettings.put("kafka_consumer_id", "default_cloudpelican_lsd_consumer");
        }

        // Load full settings from file
        JsonObject settings;
        if (lightSettings.containsKey("conf_path")) {
            String json = FileUtils.readFileToString(new File(lightSettings.get("conf_path")));
            JsonParser jp = new JsonParser();
            settings = jp.parse(json).getAsJsonObject();
        } else {
            // Default empty
            settings = new JsonObject();
        }

        // Add light settings to json
        for (Map.Entry<String, String> kv : lightSettings.entrySet()) {
            settings.addProperty(kv.getKey(), kv.getValue());
        }

        // Debug
        LOG.info(settings.toString());

        // Topology
        TopologyBuilder builder = new TopologyBuilder();
        int globalConcurrency = 1;

        // Time
        TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));

        // Read from kafka

        BrokerHosts hosts = new ZkHosts(lightSettings.get("zookeeper_nodes"));
        SpoutConfig spoutConfig = new SpoutConfig(hosts, lightSettings.get("kafka_topic"), "/" + lightSettings.get("kafka_topic"), lightSettings.get("kafka_consumer_id"));
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout(KAFKA_SPOUT, kafkaSpout, 3);

        // Match bolt
        builder.setBolt(MATCH_BOLT, new MatchBolt(lightSettings), globalConcurrency * 8).localOrShuffleGrouping(KAFKA_SPOUT);

        // Error classifier bolt
        builder.setBolt(ERROR_CLASSIFIER_BOLT, new ErrorClassifierBolt(lightSettings), globalConcurrency * 8).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor result writer bolt
        builder.setBolt(SUPERVISOR_RESULT_WRITER, new SupervisorResultWriterBolt(lightSettings), globalConcurrency * 4).fieldsGrouping(MATCH_BOLT, new Fields("filter_id"));

        // Supervisor stats writer bolt
        builder.setBolt(SUPERVISOR_STATS_WRITER, new SupervisorStatsWriterBolt(lightSettings), globalConcurrency * 2).fieldsGrouping(MATCH_BOLT, "match_stats", new Fields("filter_id"));
        builder.setBolt(SUPERVISOR_ERROR_STATS_WRITER, new SupervisorStatsWriterBolt(lightSettings), globalConcurrency * 2).fieldsGrouping(ERROR_CLASSIFIER_BOLT, "error_stats", new Fields("filter_id"));

        // Debug on for testing
        Config conf = new Config();
        conf.setDebug(false);
        String topologyName = lightSettings.getOrDefault("topology_name", "cloudpelican_stormprocessor");
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
