package de.julielab.xmlData.dataBase;

import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import de.julielab.xmlData.config.FieldConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Map;
import java.util.concurrent.Exchanger;

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
    private DataBaseConnector dbc;

    // To query by PMID, uses two other threads
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String table, String schemaName) {
        this.dbc = dbc;
        String[] tables = new String[1];
        String[] schemaNames = new String[1];
        tables[0] = table;
        schemaNames[0] = schemaName;
        backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, null, schemaNames);
        update();
    }

    // To query by PMID, uses two other threads
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String table, String whereClause, String schemaName) {
        this.dbc = dbc;
        String[] tables = new String[1];
        String[] schemaNames = new String[1];
        tables[0] = table;
        schemaNames[0] = schemaName;
        backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, whereClause, schemaNames);
        update();
    }

    /**
     * Retrieves data from the database over multiple tables. All tables will be joined on the given IDs.
     * The columns to be retrieved for each table is determined by its table schema. For this purpose, the
     * <code>tables</code> and <code>schemaName</code> arrays are required to be parallel.
     *
     * @param ids         A list of primary keys identifying the items to retrieve.
     * @param tables      The tables from which the items should be retrieved that are identified by <code>ids</code>.
     * @param schemaNames A parallel array to <code>tables</code> thas specifies the table schema name of each table.
     * @return The joined data from the requested tables.
     */
    public ThreadedColumnsToRetrieveIterator(DataBaseConnector dbc, List<Object[]> ids, String[] tables, String[] schemaNames) {
        this.dbc = dbc;
        backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, null, schemaNames);
        update();
    }

    /*
     * (non-Javadoc)
     *
     * @see de.julielab.xmlData.dataBase.DBCThreadedIterator#destroy()
     */
    @Override
    public void close() {
        super.close();
        ((ArrayResToListThread) backgroundThread).end();
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
    private class ArrayResToListThread extends Thread {
        private final ArrayFromDBThread arrayFromDBThread;
        private Exchanger<List<byte[][]>> listExchanger;
        private Exchanger<ResultSet> resExchanger = new Exchanger<ResultSet>();
        private ResultSet currentRes;
        private ArrayList<byte[][]> currentList;
        private String[] schemaName;
        private boolean joined = false;
        private volatile boolean end = false;

        ArrayResToListThread(Exchanger<List<byte[][]>> listExchanger, List<Object[]> keyList, String[] tables,
                             String whereClause, String[] schemaName) {
            this.listExchanger = listExchanger;
            this.schemaName = schemaName;
            if (tables.length > 1) {
                this.joined = true;
            }
            // start the thread that is actually querying the database
            arrayFromDBThread = new ArrayFromDBThread(resExchanger, keyList, tables, whereClause, schemaName);
            try {
                // retrieve the first result without yet running the thread;
                // when we have the result, we begin to create the result list
                // out of the first retrieved ResultSet and return the list,
                // then get the next results and so on...
                currentRes = resExchanger.exchange(null);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
                                retrievedData[i] = JulieXMLTools.unGzipData(retrievedData[i]);
                        }
                        currentList.add(retrievedData);
                    } // end ResultSet to List conversion
                    // Offer the created result list to the calling program;
                    // as soon as the result has been given away, we
                    // continue fetching more documents from the database
                    // below, allowing the calling program to process the
                    // current result and already fetching the next
                    if (!currentList.isEmpty())
                        listExchanger.exchange(currentList);
                    // Get the next ResultSet from the database
                    currentRes = resExchanger.exchange(null);
                }
                listExchanger.exchange(null); // stop signal
            } catch (InterruptedException | SQLException | IOException e) {
                LOG.error(
                        "Exception occured while reading " + "data from result set, index {}. "
                                + "Corresponding field in schema definition is: {}. Read data was: \"{}\"",
                        new Object[]{i + 1, fields.get(i), new String(retrievedData[i])});
                e.printStackTrace();
            } catch (NullPointerException e) {
                LOG.debug("NPE on: Index {}, field {}, data {}",
                        new Object[]{i, fields.get(i), retrievedData != null ? retrievedData[i] : null});
                throw e;
            }
        }

        /**
         * Must be called when the thread is no longer required. Otherwise, it will
         * continue querying the database.
         */
        public void end() {
            arrayFromDBThread.end();
            end = true;
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
    private class ArrayFromDBThread extends Thread {
        private Iterator<Object[]> keyIter;
        private Exchanger<ResultSet> resExchanger;
        private StringBuilder queryBuilder;
        private ResultSet currentRes;
        private String selectFrom;
        private Connection conn;
        private String whereClause = null;
        private FieldConfig fieldConfig;
        private volatile boolean end;
        private boolean joined = false;
        private String dataTable;
        private String dataSchema;

        public ArrayFromDBThread(Exchanger<ResultSet> resExchanger, List<Object[]> keyList, String[] table,
                                 String whereClause, String[] schemaName) {
            this.conn = dbc.getConn();
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
                ArrayList<String> select = new ArrayList<String>();
                ArrayList<String> leftJoin = new ArrayList<String>();

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
                LOG.trace("Querying data via SQL: {}", selectFrom);
            }
        }

        /**
         * Fetches results as long as there are unprocessed documents in the given
         * subset table.
         */
        public void run() {
            try {
                while (keyIter.hasNext() && !end) {
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
                List<String> pkConjunction = new ArrayList<String>();
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
            // The connection is closed automatically when the thread ends, see
            // the run() method.
            end = true;
        }

    }
}
