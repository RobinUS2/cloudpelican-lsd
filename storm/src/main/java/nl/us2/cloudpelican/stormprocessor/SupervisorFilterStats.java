package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 15/06/15.
 */
public class SupervisorFilterStats {
    private String filterId;
    private long count;
    private int metric;
    private long bucket;
    public SupervisorFilterStats(String filterId, int metric, long bucket) {
        this.filterId = filterId;
        this.count = 0L;
        this.metric = metric;
        this.bucket = bucket;
    }

    public void increment(long increment ){
        this.count += increment;
    }

    public long getCount() {
        return count;
    }

    public int getMetric() {
        return metric;
    }

    public long getBucket() {
        return bucket;
    }

    public String getFilterId() {
        return filterId;
    }

    public String toKey() {
        return SupervisorFilterStats.getKey(getFilterId(), getMetric(), getBucket());
    }

    public static String getKey(String filterId, int metric, long bucket) {
        return "f_" + filterId + "_m" + metric +"_b" + bucket;
    }
}
