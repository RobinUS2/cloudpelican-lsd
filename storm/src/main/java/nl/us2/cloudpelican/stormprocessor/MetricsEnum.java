package nl.us2.cloudpelican.stormprocessor;

/**
 * Created by robin on 15/06/15.
 */
public enum MetricsEnum {
    MATCH (1),
    ERRROR (2);

    private final int mask;

    MetricsEnum(int mask)
    {
        this.mask = mask;
    }

    public int getMask()
    {
        return mask;
    }
}
