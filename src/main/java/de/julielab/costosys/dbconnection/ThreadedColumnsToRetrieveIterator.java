package de.julielab.costosys.dbconnection;

import de.julielab.costosys.configuration.FieldConfig;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipException;

/**
 * An iterator that returns documents stored in the database identified by the
 * primary keys delivered in a list upon creation of the iterator. The returned
 * data corresponds to the columns marked to be retrieved in the table schema also given at iterator creation.<br>
 * The iterator employs two threads to retrieve new documents from the database
 * while other documents, fetched before, can be processed concurrently. The
 * idea is that the database can work in parallel to the program working with
 * the retrieved documents.
 *
 * @author hellrich
 */
public class ThreadedColumnsToRetrieveIterator extends DBCThreadedIterator<byte[][]> {
    private final static Logger LOG = LoggerFactory.getLogger(ThreadedColumnsToRetrieveIterator.class);
    private static int arrayResToListThreadCounter;
    private static int arrayFromDBThreadCounter;
    private DataBaseConnector dbc;

    // To query by PMID, uses two other threads
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, CoStoSysConnection conn, List<Object[]> ids, String table, String schemaName) {
        this.dbc = dbc;
        String[] tables = new String[1];
        String[] schemaNames = new String[1];
        tables[0] = table;
        schemaNames[0] = schemaName;
        backgroundThread = new ArrayResToListThread(conn, listExchanger, ids, tables, null, schemaNames);
        update();
    }

    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String table, String schemaName) {
        this(dbc, null, ids, table, schemaName);
    }

    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String table, String whereClause, String schemaName) {
        this(dbc, null, ids, table, whereClause, schemaName);
    }

    // To query by PMID, uses two other threads
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, CoStoSysConnection conn, List<Object[]> ids, String table, String whereClause, String schemaName) {
        this.dbc = dbc;
        String[] tables = new String[1];
        String[] schemaNames = new String[1];
        tables[0] = table;
        schemaNames[0] = schemaName;
        backgroundThread = new ArrayResToListThread(conn, listExchanger, ids, tables, whereClause, schemaNames);
        update();
    }

    /**
     * Retrieves data from the database over multiple tables. All tables will be joined on the given IDs.
     * The columns to be retrieved for each table is determined by its table schema. For this purpose, the
     * <code>tables</code> and <code>schemaName</code> arrays are required to be parallel.
     *
     * @param dbc         A DataBaseConnector instance
     * @param conn        An active database connection
     * @param ids         A list of primary keys identifying the items to retrieve.
     * @param tables      The tables from which the items should be retrieved that are identified by <code>ids</code>.
     * @param schemaNames A parallel array to <code>tables</code> thas specifies the table schema name of each table.
     * @return The joined data from the requested tables.
     */
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, CoStoSysConnection conn, List<Object[]> ids, String[] tables, String[] schemaNames) {
        this.dbc = dbc;
        backgroundThread = new ArrayResToListThread(conn, listExchanger, ids, tables, null, schemaNames);
        update();
    }

    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String[] tables, String[] schemaNames) {
        this(dbc, null, ids, tables, schemaNames);
    }

    /*
     * (non-Javadoc)
     *
     * @see DBCThreadedIterator#destroy()
     */
    @Override
    public void close() {
        LOG.debug("Iterator with background thread {} is closing.", ((Thread)backgroundThread).getName());
        ((ArrayResToListThread) backgroundThread).end();
    }

    @Override
    public void join() throws InterruptedException {
        ((ArrayResToListThread) backgroundThread).join();
    }


    /**
     * This class converts a <tt>ResultSet</tt>, retrieved from the database, into a
     * list, that is then returned.<br>
     * This class is a <tt>Thread</tt> and serves as an intermediate layer between
     * the program that uses the resulting list and another thread that is doing the
     * actual database querying. This class has to {@link Exchanger}: One
     * <tt>Exchanger</tt> for the result list that is returned to the caller.
     * Another <tt>Exchanger</tt> is used to retrieve database results in the form
     * of {@link ResultSet} instances from the thread querying the database. In
     * between, this class converts the <tt>ResultSet</tt> to a <tt>List</tt>.
     *
     * @author hellrich
     */
    private class ArrayResToListThread extends Thread implements ConnectionClosable {
        private final Logger log = LoggerFactory.getLogger(ArrayResToListThread.class);
        private final ArrayFromDBThread arrayFromDBThread;
        private Exchanger<List<byte[][]>> listExchanger;
        private Exchanger<ResultSet> resExchanger = new Exchanger<>();
        private ResultSet currentRes;
        private ArrayList<byte[][]> currentList;
        private String[] schemaName;
        private boolean joined = false;
        private volatile boolean end = false;

        ArrayResToListThread(CoStoSysConnection conn, Exchanger<List<byte[][]>> listExchanger, List<Object[]> keyList, String[] tables,
                             String whereClause, String[] schemaName) {
            this.listExchanger = listExchanger;
            this.schemaName = schemaName;
            if (tables.length > 1) {
                this.joined = true;
            }
            // start the thread that is actually querying the database (it starts itself in its constructor)
            arrayFromDBThread = new ArrayFromDBThread(conn, resExchanger, keyList, tables, whereClause, schemaName);
            try {
                // the 'arrayFromDBThread' is already running to produce the first result;
                // retrieve the first result without yet running this thread;
                // when we have the result, we begin to create the result list
                // out of the first retrieved ResultSet and return the list,
                // then get the next results and so on...
                currentRes = resExchanger.exchange(null);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            setName("ArrayResultToListThread-" + ++arrayResToListThreadCounter);
            setDaemon(true);
            start();
        }

        @SuppressWarnings("unchecked")
        public void run() {
            Pair<Integer, List<Map<String, String>>> numColumnsAndFields = dbc.getNumColumnsAndFields(joined, schemaName);
            int numColumns = numColumnsAndFields.getLeft();
            List<Map<String, String>> fields = numColumnsAndFields.getRight();
            int i = 0;
            byte[][] retrievedData = null;
            try {
                while (currentRes != null && !end) {
                    currentList = new ArrayList<>();
                    // convert the current database ResultSet into a list.
                    while (currentRes.next()) {
                        retrievedData = new byte[numColumns][];
                        for (i = 0; i < retrievedData.length; i++) {
                            retrievedData[i] = currentRes.getBytes(i + 1);
                            if (Boolean.parseBoolean(fields.get(i).get(JulieXMLConstants.GZIP))
                                    && retrievedData[i] != null)
                                try {
                                    retrievedData[i] = JulieXMLTools.unGzipData(retrievedData[i]);
                                } catch (ZipException e) {
                                    // It seems to configuration did not match the actual data. Perhaps the gzip flag
                                    // was just wrong, we will issue a warning and continue
                                    log.warn("Got error with message {}. The data for column {} is flagged to be in GZIP format but it wasn't. The original data is returned instead of un-gzipped data.",e.getMessage(), fields.get(i).get(JulieXMLConstants.NAME));
                                }
                        }
                        currentList.add(retrievedData);
                    } // end ResultSet to List conversion
                    // Offer the created result list to the calling program;
                    // as soon as the result has been given away, we
                    // continue fetching more documents from the database
                    // below, allowing the calling program to process the
                    // current result and already fetching the next
                    if (!currentList.isEmpty()) {
                        log.debug("Sending current result list of size {} to iterator", currentList.size());
                        listExchanger.exchange(currentList);
                    }
                    // Get the next ResultSet from the database
                    currentRes = resExchanger.exchange(null);
                }
                log.debug("Signalling end of data to iterator (data for all keys received)");
                final Timer t = new Timer(getName());
                t.start();
                listExchanger.exchange(null); // stop signal
                t.interrupt();
            } catch (InterruptedException | SQLException | IOException e) {
                log.error(
                        "Exception occurred while reading " + "data from result set, index {}. "
                                + "Corresponding field in schema definition is: {}. Read data was: \"{}\"",
                        new Object[]{i + 1, fields.get(i), new String(retrievedData[i], StandardCharsets.UTF_8)});
                byte[][] d = retrievedData;
                log.error("All retrieved data was {}", IntStream.rangeClosed(0, i).mapToObj(j -> new String(d[j], StandardCharsets.UTF_8)).collect(Collectors.toList()));
                e.printStackTrace();
            } catch (NullPointerException e) {
                log.debug("NPE on: Index {}, field {}, data {}",
                        new Object[]{i, fields.get(i), retrievedData != null ? retrievedData[i] : null});
                throw e;
            }
            log.debug("{} has finished", getName());
        }

        /**
         * Must be called when the thread is no longer required. Otherwise, it will
         * continue querying the database.
         */
        public void end() {
            log.debug("Ending thread {}", getName());
            arrayFromDBThread.end();
            end = true;
        }

        @Override
        public void closeConnection() {
            arrayFromDBThread.closeConnection();
        }
    }

    /**
     * This class is last <tt>Thread</tt> in the
     * <code>Iterator - ResultSet to List converter - ResultSet from database retriever</code>
     * chain and thus is the class doing the database querying. <br>
     * Upon creation, this class starts itself as a demon <tt>Thread</tt>. It
     * queries {@link DataBaseConnector#queryBatchSize} IDs and offers the
     * <tt>ResultSet</tt> in an {@link Exchanger} for the intermediate
     * <tt>Thread</tt>.
     *
     * @author hellrich
     */
    private class ArrayFromDBThread extends Thread implements ConnectionClosable {
        private final Logger log = LoggerFactory.getLogger(ArrayFromDBThread.class);
        private final boolean externalConnectionGiven;
        private Iterator<Object[]> keyIter;
        private Exchanger<ResultSet> resExchanger;
        private StringBuilder queryBuilder;
        private ResultSet currentRes;
        private String selectFrom;
        private CoStoSysConnection conn;
        private String whereClause = null;
        private FieldConfig fieldConfig;
        private volatile boolean end;
        private boolean joined = false;
        private String dataTable;
        private String dataSchema;

        public ArrayFromDBThread(CoStoSysConnection conn, Exchanger<ResultSet> resExchanger, List<Object[]> keyList, String[] table,
                                 String whereClause, String[] schemaName) {
            externalConnectionGiven = conn != null;
            this.conn = conn;
            this.resExchanger = resExchanger;
            keyIter = keyList.iterator();
            this.queryBuilder = new StringBuilder();
            this.whereClause = whereClause;
            this.dataTable = table[0];
            this.dataSchema = schemaName[0];
            if (table.length > 1 && schemaName.length > 1) {
                this.joined = true;
            }
            buildSelectFrom(table, schemaName);
            setName("ArrayFromDBThread-" + ++arrayFromDBThreadCounter);
            setDaemon(true);
            start();
        }

        /**
         * Create the basic SQL query structure used to query documents from the
         * database.
         *
         * @param table
         * @param schemaName
         */
        private void buildSelectFrom(String[] table, String[] schemaName) {
            // Build SELECT if there is only one table.
            if (!joined) {
                fieldConfig = dbc.getFieldConfiguration(dataSchema);
                selectFrom = "SELECT " + StringUtils.join(fieldConfig.getColumnsToRetrieve(), ",") + " FROM "
                        + dataTable + " WHERE ";
                // Build SELECT if multiple tables will be joined.
                // This will be in the form
                // 'SELECT dataTable.pmid, otherTable1.data, otherTable2.data
                // FROM dataTable
                // LEFT JOIN otherTable1 ON dataTable.pmid=otherTable1.pmid
                // LEFT JOIN otherTable2 ON dataTable.pmid=othertable2.pmid
                // WHERE (dataTable.pmid=1) OR (dataTable.pmid=2) OR ...'
            } else {
                String[] primaryKey = null;
                List<String> select = new ArrayList<>();
                List<String> leftJoin = new ArrayList<>();

                for (int i = 0; i < table.length; i++) {
                    fieldConfig = dbc.getFieldConfiguration(schemaName[i]);
                    String[] columnsToRetrieve = fieldConfig.getColumnsToRetrieve();
                    for (int j = 0; j < columnsToRetrieve.length; j++) {
                        String column = table[i] + "." + columnsToRetrieve[j];
                        select.add(column);
                    }
                    if (i == 0) {
                        // Get the names of the primary keys once, since they
                        // should be identical for all tables.
                        primaryKey = fieldConfig.getPrimaryKey();
                    } else {
                        String primaryKeyMatch = "";
                        for (int j = 0; j < primaryKey.length; j++) {
                            primaryKeyMatch = table[0] + "." + primaryKey[j] + "=" + table[i] + "." + primaryKey[j];
                            if (!(j == primaryKey.length - 1))
                                primaryKeyMatch = primaryKeyMatch + " AND ";
                        }
                        String join = "LEFT JOIN " + table[i] + " ON " + primaryKeyMatch;
                        leftJoin.add(join);
                    }
                }
                selectFrom = "SELECT " + StringUtils.join(select, ",") + " FROM " + table[0] + " "
                        + StringUtils.join(leftJoin, " ") + " WHERE ";
                log.trace("Querying data via SQL: {}", selectFrom);
            }
        }

        /**
         * Fetches results as long as there are unprocessed documents in the given
         * subset table.
         */
        public void run() {
            if (!externalConnectionGiven)
                this.conn = dbc.obtainOrReserveConnection(true);
            else
                this.conn.incrementUsageNumber();
            try {
                while (keyIter.hasNext() && !end) {
                    currentRes = getFromDB();
                    resExchanger.exchange(currentRes);
                }
                resExchanger.exchange(null); // Indicates end
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                closeConnection();
            }
            log.debug("{} has finished", getName());
        }

        /**
         * Builds the final SQL query specifying the exact primary keys for retrieval,
         * performs the actual query and returns the respective <tt>ResultSet</tt>
         *
         * @return
         */
        private ResultSet getFromDB() {
            ResultSet res = null;
            String sql = null;
            try {
                queryBuilder.delete(0, queryBuilder.capacity());
                Statement stmt = conn.createStatement();
                List<String> pkConjunction = new ArrayList<>();
                for (int i = 0; keyIter.hasNext() && i < dbc.getQueryBatchSize(); ++i) {
                    // get the next row of primary key values, e.g.
                    //
                    // pmid | systemID
                    // --------------------
                    // 1564 | FSU <--- this is stored in "keys"
                    Object[] keys = keyIter.next();
                    String[] nameValuePairs;
                    nameValuePairs = new String[keys.length];
                    // build an array of pairs like
                    // ["pmid = 1563", "systemID = FSU"]
                    if (!joined) {
                        for (int j = 0; j < keys.length; ++j) {
                            String fieldName = fieldConfig.getPrimaryKey()[j];
                            nameValuePairs[j] = String.format("%s = '%s'", fieldName, keys[j]);
                        }
                    } else {
                        for (int j = 0; j < keys.length; ++j) {
                            String fieldName = fieldConfig.getPrimaryKey()[j];
                            nameValuePairs[j] = String.format("%s = '%s'", dataTable + "." + fieldName, keys[j]);
                        }
                    }
                    // make a conjunction of the name value pairs:
                    // "(pmid = 1563 AND systemID = FSU)"
                    pkConjunction.add("(" + StringUtils.join(nameValuePairs, " AND ") + ")");
                }
                queryBuilder.append(selectFrom);
                queryBuilder.append("(");
                queryBuilder.append(StringUtils.join(pkConjunction, " OR "));
                queryBuilder.append(")");
                if (whereClause != null)
                    queryBuilder.append(" AND " + whereClause);
                sql = queryBuilder.toString();
                LOG.trace("Fetching data with command \"{}\"", sql);
                res = stmt.executeQuery(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                LOG.error("SQL: " + sql);
            }
            return res;
        }

        public void end() {
            end = true;
            closeConnection();
        }

        @Override
        public void closeConnection() {
            log.trace("Closing connection {}", conn.getConnection());
            conn.close();
        }
    }
    private class Timer extends Thread {
        private String threadName;

        public Timer(String threadName) {

            this.threadName = threadName;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(300000);
            LOG.debug("Waited 5 minutes for iterator for thread {}", threadName);
            } catch (InterruptedException e) {
                // that's alright
            }
        }
    }
}
