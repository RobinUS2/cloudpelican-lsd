package nl.us2.cloudpelican.stormprocessor;

import java.util.List;

/**
 * Created by robin on 20/07/15.
 */
public class LogSniffResult {
    private List<LogTypes> types;
    private String dateStr;

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setTypes(List<LogTypes> types) {
        this.types = types;
    }

    public List<LogTypes> getTypes() {
        return types;
    }
}
