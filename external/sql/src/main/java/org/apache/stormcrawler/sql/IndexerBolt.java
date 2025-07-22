package org.apache.stormcrawler.sql;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores URL and selected metadata into a SQL table * */
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

    public static final String SQL_INDEX_TABLE_PARAM_NAME = "sql.index.table";

    private OutputCollector _collector;
    private MultiCountMetric eventCounter;
    private Connection connection;
    private String tableName;
    private Map conf;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
        this.eventCounter = context.registerMetric("SQLIndexer", new MultiCountMetric(), 10);
        this.tableName = ConfUtils.getString(conf, SQL_INDEX_TABLE_PARAM_NAME);
        this.conf = conf;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        String normalisedurl = valueForURL(tuple);
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        try {
            Map<String, String[]> keyVals = filterMetadata(metadata);
            Object[] keys = keyVals.keySet().toArray();

            StringBuilder fieldsBuilder = new StringBuilder(fieldNameForURL());
            StringBuilder placeholdersBuilder = new StringBuilder("?");
            StringBuilder updatesBuilder = new StringBuilder();

            for (int i = 0; i < keys.length; i++) {
                String key = (String) keys[i];
                fieldsBuilder.append(", ").append(key);
                placeholdersBuilder.append(", ?");
                if (i > 0) updatesBuilder.append(", ");
                updatesBuilder.append(key).append("=VALUES(").append(key).append(")");
            }

            String sql = String.format(
                    Locale.ROOT,
                    "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                    tableName,
                    fieldsBuilder,
                    placeholdersBuilder,
                    updatesBuilder
            );

            if (connection == null) {
                try {
                    connection = SQLUtil.getConnection(conf);
                } catch (SQLException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                }
            }

            LOG.debug("PreparedStatement => {}", sql);
            PreparedStatement preparedStmt = connection.prepareStatement(sql);

            // Set URL as first parameter
            preparedStmt.setString(1, normalisedurl);

            // Set metadata values
            for (int i = 0; i < keys.length; i++) {
                insert(preparedStmt, i + 2, (String) keys[i], keyVals);
            }

            preparedStmt.executeUpdate();
            preparedStmt.close();

            eventCounter.scope("Indexed").incrBy(1);
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);

        } catch (Exception e) {
            LOG.error("Error inserting into SQL", e);
            _collector.fail(tuple);
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e1) {
                    // ignore
                }
                connection = null;
            }
        }
    }

    private void insert(PreparedStatement preparedStmt, int position, String label, Map<String, String[]> keyVals)
            throws SQLException {
        String[] values = keyVals.get(label);
        String value = "";
        if (values == null || values.length == 0) {
            LOG.info("No values found for label {}", label);
        } else {
            value = values[0];
            if (values.length > 1) {
                LOG.info("More than one value found for label {}", label);
            }
        }
        preparedStmt.setString(position, value);
    }
}
