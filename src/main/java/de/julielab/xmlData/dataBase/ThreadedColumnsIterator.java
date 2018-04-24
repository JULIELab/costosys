package de.julielab.xmlData.dataBase;

import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import de.julielab.xmlData.config.FieldConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.function.Function;

/**
 * Retrieves a single fields from the requested table. Uses a background thread for fetching the next batch of fields
 * after the previous batch has been returned.
 *
 * @author hellrich
 */
public class ThreadedColumnsIterator extends DBCThreadedIterator<Object[]> {
    private final static Logger LOG = LoggerFactory.getLogger(ThreadedColumnsIterator.class);
    private DataBaseConnector dbc;

    // To query by PMID, uses two other threads
    public ThreadedColumnsIterator(DataBaseConnector dbc, List<String[]> keys, List<String> fields, String table, String schemaName) {
        this.dbc = dbc;
        backgroundThread = new ResToListThread(listExchanger, keys, fields, table, schemaName);
        update();
    }

    // To query all, uses only one other thread
    public ThreadedColumnsIterator(DataBaseConnector dbc, List<String> fields, String table) {
        this.dbc = dbc;
        backgroundThread = new ListFromDBThread(listExchanger, fields, table);
        update();
    }

    /**
     * A second thread, making a list out of ResultSets
     *
     * @author hellrich
     */
    private class ResToListThread extends Thread {
        private final List<String> fields;
        private Exchanger<List<Object[]>> listExchanger;
        private Exchanger<ResultSet> resExchanger = new Exchanger<ResultSet>();
        private ResultSet currentRes;
        private List<Object[]> currentList;

        ResToListThread(Exchanger<List<Object[]>> listExchanger, List<String[]> keys, List<String> fields, String table,
                        String schemaName) {
            this.listExchanger = listExchanger;
            this.fields = fields;
            new FromDBThread(resExchanger, keys, this.fields, table, schemaName);
            try {
                currentRes = resExchanger.exchange(null);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            setDaemon(true);
            start();
        }

        public void run() {
            Function<String, Boolean> unzip = fieldName -> Boolean
                    .parseBoolean(dbc.getFieldConfiguration(dbc.getActiveTableSchema()).getField(fieldName).get(JulieXMLConstants.GZIP));
            try {
                while (currentRes != null) {
                    currentList = new ArrayList<>();
                    while (currentRes.next()) {
                        Object[] columnValues = new Object[fields.size()];
                        for (int i = 0; i < fields.size(); ++i) {
                            Object bytes = currentRes.getObject(i + 1);
                            if (unzip.apply(fields.get(i)))
                                bytes = JulieXMLTools.unGzipData((byte[])bytes);
                            columnValues[i] = bytes;
                        }
                        currentList.add(columnValues);
                    }
                    if (!currentList.isEmpty())
                        listExchanger.exchange(currentList);
                    currentRes = resExchanger.exchange(null);
                }
                listExchanger.exchange(null); // stop signal
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A third thread, querying the database for ResultSets
     *
     * @author hellrich
     */
    private class FromDBThread extends Thread {
        private Iterator<String[]> keyIter;
        private Exchanger<ResultSet> resExchanger;
        private String statement;
        private ResultSet currentRes;
        private Connection conn;
        private FieldConfig fieldConfig;

        public FromDBThread(Exchanger<ResultSet> resExchanger, List<String[]> keys, List<String> fields, String table,
                            String schemaName) {
            this.conn = dbc.getConn();
            this.resExchanger = resExchanger;
            this.fieldConfig = dbc.getFieldConfiguration(schemaName);
            statement = "SELECT " + StringUtils.join(fields, ",") + " FROM " + table + " WHERE ";
            keyIter = keys.iterator();
            setDaemon(true);
            start();
        }

        public void run() {
            try {
                while (keyIter.hasNext()) {
                    currentRes = getFromDB();
                    resExchanger.exchange(currentRes);
                }
                resExchanger.exchange(null); // Indicates end
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private ResultSet getFromDB() {
            ResultSet rs = null;
            StringBuilder sql = new StringBuilder(statement);
            Object[] values = null;
            try {
                Statement st = conn.createStatement();
                String[] pks = fieldConfig.getPrimaryKey();

                int i = 0;
                while (keyIter.hasNext() && i < dbc.getQueryBatchSize()) {
                    values = keyIter.next();
                    for (int j = 0; j < pks.length; ++j) {
                        if (fieldConfig.isOfStringType(pks[j]))
                            sql.append(pks[j]).append("=\'").append(values[j]).append("\'");
                        else
                            sql.append(pks[j]).append("=").append(values[j]);
                        if (j < pks.length - 1)
                            sql.append(" AND ");
                    }
                    sql.append(" OR ");
                    ++i;
                }
                sql.delete(sql.length() - 4, sql.length()); // Remove trailing
                // " OR "
                rs = st.executeQuery(sql.toString());
            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println(sql.toString());
            } catch (ArrayIndexOutOfBoundsException e) {
                LOG.error("Configuration file and query are incompatible.");
                String wrongLine = "";
                if (values != null)
                    for (int i = 0; i < values.length; ++i)
                        wrongLine += values[i];
                LOG.error("Error in line beginning with: " + wrongLine);
            }
            return rs;
        }

    }

    /**
     * Variant, a thread querying the whole database for ResultSets, returning all
     * entries (as lists) to the iterator (replaces threads 2 and 3)
     *
     * @author hellrich
     */
    private class ListFromDBThread extends Thread {
        private final List<String> fields;
        private Exchanger<List<Object[]>> listExchanger;
        private List<Object[]> currentList;
        private String selectFrom;
        private boolean hasNext;
        private ResultSet res;
        private Connection conn;

        public ListFromDBThread(Exchanger<List<Object[]>> listExchanger, List<String> fields, String table) {
            this.listExchanger = listExchanger;
            this.fields = fields;
            selectFrom = String.format("SELECT %s FROM %s", StringUtils.join(fields, ","), table);
            try {
                conn = dbc.getConn();
                conn.setAutoCommit(false);// cursor doesn't work otherwise
                Statement st = conn.createStatement();
                st.setFetchSize(dbc.getQueryBatchSize()); // cursor
                res = st.executeQuery(selectFrom);
                hasNext = res.next(); // never forget
                updateCurrentList();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            setDaemon(true);
            start();
        }

        public void run() {
            try {
                listExchanger.exchange(currentList);
                while (hasNext) {
                    updateCurrentList();
                    listExchanger.exchange(currentList);
                }
                conn.setAutoCommit(true);
                listExchanger.exchange(new ArrayList<>()); // Empty
                // lists
                // as
                // stop
                // signal
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private void updateCurrentList() {
            currentList = new ArrayList<>();
            Function<String, Boolean> unzip = fieldName -> Boolean
                    .parseBoolean(dbc.getFieldConfiguration(dbc.getActiveTableSchema()).getField(fieldName).get(JulieXMLConstants.GZIP));
            int i = 0;
            try {
                while (hasNext && i < dbc.getQueryBatchSize()) {
                    currentList = new ArrayList<>();
                    Object[] columnValues = new Object[fields.size()];
                    for (int j = 0; j < fields.size(); ++j) {
                        Object o = res.getObject(j + 1);
                        // When the field is gzipped, its type should by bytea so the cast should work
                        if (unzip.apply(fields.get(j)))
                            o = JulieXMLTools.unGzipData((byte[])o);
                        columnValues[j] = o;
                    }
                    currentList.add(columnValues);
                    ++i;
                    hasNext = res.next();
                }
            } catch (SQLException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}