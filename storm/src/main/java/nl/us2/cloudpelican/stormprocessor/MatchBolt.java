package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 07/06/15.
 */
import backtype.storm.Config;
import backtype.storm.messaging.local;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.auth.AuthScope;
import org.apache.storm.http.auth.Credentials;
import org.apache.storm.http.auth.UsernamePasswordCredentials;
import org.apache.storm.http.client.CredentialsProvider;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpGet;
import org.apache.storm.http.impl.client.BasicCredentialsProvider;
import org.apache.storm.http.impl.client.HttpClientBuilder;
import org.apache.storm.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;
//import backtype.storm.tuple.Values;
//import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;

/**
 *
 * @author robin
 */
public class MatchBolt extends BaseRichBolt {

    OutputCollector _collector;
    HashMap<String, Filter> filters;
    JsonParser jsonParser;
    private boolean localMode = false;
    private String regex;
    private HashMap<String, String> settings;

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public MatchBolt(HashMap<String, String> settings) {
        super();
        filters = null;
        this.settings = settings;
        this.regex = this.settings.get("match_regex");


    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        jsonParser = new JsonParser();

        // Local mode
        if (regex != null && !regex.trim().isEmpty()) {
            LOG.info("Setting up local regex " + regex);
            localMode = true;
            JsonObject obj = new JsonObject();
            String fakeId = UUID.randomUUID().toString();
            obj.addProperty("id", fakeId);
            obj.addProperty("regex", regex);
            filters = new HashMap<String, Filter>();
            filters.put(fakeId, new Filter(obj));
            LOG.info("Setup up local regex " + regex);
        }
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
        // Do not execute in local mode
        if (localMode) {
            return;
        }

        // Init
        if (filters == null) {
            filters = new HashMap<String, Filter>();
        }

        // Load
        try {
            HashMap<String, Filter> tmp = new HashMap<String, Filter>();
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.get("supervisor_username"), settings.get("supervisor_password")));
            HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();

            String url = settings.get("supervisor_host") + "filter";
            LOG.info(url);
            HttpGet get = new HttpGet(url);

            HttpResponse resp = client.execute(get);
            String body = EntityUtils.toString(resp.getEntity());
            LOG.debug(body);
            JsonObject outer = jsonParser.parse(body).getAsJsonObject();
            JsonArray arr = outer.get("filters").getAsJsonArray();
            for (JsonElement elm : arr) {
                try {
                    JsonObject filter = elm.getAsJsonObject();
                    Filter f = new Filter(filter);
                    if (!filters.containsKey(f.toString())) {
                        LOG.info("Loaded filter " + filter.toString());
                    }
                    tmp.put(f.Id(), f);
                } catch (Exception fe) {
                    LOG.error("Failed to load filter", fe);
                    fe.printStackTrace();
                }
            }

            // Swap
            filters = tmp;
        } catch (Exception e) {
            LOG.error("Failed to load filters", e);
            e.printStackTrace();
        }
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
        for (Filter filter : getFilters().values()) {
            Matcher m = filter.Matcher(msg);
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