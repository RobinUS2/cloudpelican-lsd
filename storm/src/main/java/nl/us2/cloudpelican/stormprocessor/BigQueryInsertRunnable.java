package nl.us2.cloudpelican.stormprocessor;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by robin on 16/07/15.
 */
public class BigQueryInsertRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySinkBolt.class);

    private final BigQuerySinkBolt parent;
    private final String filterId;
    private final ArrayList<TableDataInsertAllRequest.Rows> rows;

    public BigQueryInsertRunnable(BigQuerySinkBolt parent, String filterId, ArrayList<TableDataInsertAllRequest.Rows> rows) {
        this.parent = parent;
        this.filterId = filterId;
        this.rows = rows;
    }

    public void run() {
        try {
            // Prepare target table
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date now = new Date();
            String date = sdf.format(now);
            String targetTable = filterId + "_results_" + date + "_v" + parent.TABLE_STRUCTURE_VERSION;
            targetTable = targetTable.replace('-', '_');
            parent.prepareTable(targetTable);

            // Execute
            TableDataInsertAllRequest ir = new TableDataInsertAllRequest().setRows(rows);
            TableDataInsertAllResponse response = parent.bigquery.tabledata().insertAll(parent.projectId, parent.datasetId, targetTable, ir).execute();
            List<TableDataInsertAllResponse.InsertErrors> errors = response.getInsertErrors();
            if (errors != null) {
                LOG.error(errors.size() + " error(s) while writing " + filterId + " to BigQuery");
            } else {
                // Log lines for debug
                LOG.info(rows.size() + " lines written for " + filterId);
            }
        } catch (Exception e) {
            LOG.error("Failed to write to BigQuery", e);
        }
    }
}
