package nl.us2.cloudpelican.stormprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by robin on 20/07/15.
 */
public class LogTypeSniffer {
    private Pattern iso86001tspattern;
    private Matcher iso86001tsmatcher;

    public LogTypeSniffer() {
        // 2001-07-04T12:08:56.235-07:00
        // SimpleDateFormat yyyy-MM-dd'T'HH:mm:ss.SSSXXX
        iso86001tspattern = Pattern.compile("[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]+(\\+|\\-)[0-9]{2}:[0-9]{2}");
        iso86001tsmatcher = iso86001tspattern.matcher("");
    }

    public LogSniffResult sniff(String msg) {
        // Result holder
        LogSniffResult res = new LogSniffResult();

        // List of logtypes
        List<LogTypes> list = new ArrayList<LogTypes>();

        // Init matcher
        iso86001tsmatcher.reset(msg);

        // Match?
        if (iso86001tsmatcher.find()) {
            list.add(LogTypes.RSYSLOG);
            res.setDateStr(iso86001tsmatcher.group());
        }

        // Add list and return
        res.setTypes(list);
        return res;
    }
}
