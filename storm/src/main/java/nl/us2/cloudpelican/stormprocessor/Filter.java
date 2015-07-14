package nl.us2.cloudpelican.stormprocessor;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by robin on 07/06/15.
 */
public class Filter {
    private final JsonObject obj;
    private final String id;
    private final String name;
    private Pattern pattern;
    private Matcher matcher;
    private boolean useIndexOf = false;
    private boolean lowercase = false;
    private String regexStr;

    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    public Filter(JsonObject obj) {
        this.obj = obj;
        id = obj.get("id").getAsString();
        name = obj.get("name").getAsString();
        regexStr = obj.get("regex").getAsString();
    }

    public void compileRegex() {
        // Is this just a word or so to match? (supports also the case-insensitive flag)
        if (Pattern.compile("^(\\(\\?i\\))?[A-Za-z0-9_\\-]+$").matcher(regexStr).matches()) {
            // Case insensitive
            if (regexStr.indexOf("(?i)") == 0) {
                LOG.info("Detected lowercase, new regex " + regexStr);
                regexStr = regexStr.replace("(?i)", "");
                lowercase = true;
            }

            // Very simple word-style matching
            if (Pattern.compile("^[A-Za-z0-9_\\-]+$").matcher(regexStr).matches()) {
                LOG.info("Matching with indexOf() enabled for " + regexStr);
                useIndexOf = true;
            }
        }

        // Compile pattern
        pattern = Pattern.compile(regexStr);
    }

    public String Id() {
        return id;
    }

    public boolean isValid() {
        // ID must be hex UUID (e.g. 1cb4978b-b2e1-44c3-a007-8c0c7fb57e82)
        if (Id().length() != 36) {
            return false;
        }

        // Is this a temp filter which is old?
        if (name.startsWith("__tmp__")) {
            String tsStr = name.substring(7);
            long ts = Long.parseLong(tsStr);
            long tsNow = new Date().getTime() / 1000L;
            int maxHours = 1;
            long tsMin = tsNow - (maxHours*3600); // after X hours auto deleted
            if (ts < tsMin) {
                LOG.info("Filter " + id + " was temporary and is declared invalid after " + maxHours + " hour(s)");
                return false;
            }
        }

        return true;
    }

    public Matcher Matcher(String msg) {
        if (matcher == null) {
            matcher = pattern.matcher(msg);
            return matcher;
        }
        return matcher.reset(msg);
    }

    public boolean matches(String msg) {
        // Lowercase?
        if (lowercase) {
            msg = msg.toLowerCase();
        }

        // Index of matching?
        if (useIndexOf) {
            // Use faster matching (2-30x) first
            if (!msg.contains(regexStr)) {
                return false;
            }
        }
        // Regex matching
        Matcher m = Matcher(msg);
        boolean b = m.find();
        return b;
    }
}
