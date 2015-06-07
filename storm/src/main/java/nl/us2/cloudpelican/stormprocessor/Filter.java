package nl.us2.cloudpelican.stormprocessor;

import com.google.gson.JsonObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by robin on 07/06/15.
 */
public class Filter {
    private JsonObject obj;
    private String id;
    private Pattern pattern;
    private Matcher matcher;

    public Filter(JsonObject obj) {
        this.obj = obj;
        id = obj.get("id").getAsString();
        pattern = Pattern.compile(obj.get("regex").getAsString());
    }

    public String Id() {
        return id;
    }

    public Matcher Matcher(String msg) {
        if (matcher == null) {
            matcher = pattern.matcher(msg);
        } else {
            matcher = matcher.reset(msg);
        }
        return matcher;
    }
}
