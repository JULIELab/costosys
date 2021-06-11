package de.julielab.costosys.dbconnection.util;

import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.xml.XmiSplitConstants;
import de.julielab.xml.binary.BinaryJedisFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class CoStoSysJedisTools {
    private final static Logger log = LoggerFactory.getLogger(CoStoSysJedisTools.class);

    /**
     * Reads the first row of the given table and returns information about whether the data is in GZIP format and/or the JeDIS binary format.
     *
     * @param dbc   A {@link DataBaseConnector} connected to the database of interest.
     * @param table The name of a table to check the data format of.
     * @param column The column to check to data format of.
     * @return A {@link JedisDataFormat} object disclosing information about the found data format.
     * @throws CoStoSysException If the given table does not exist.
     */
    public static JedisDataFormat determineDataFormat(DataBaseConnector dbc, String table, String column) throws CoStoSysException {
        boolean doGzip = true;
        boolean useBinaryFormat = true;
        String dataTable = dbc.getNextOrThisDataTable(table);
        log.debug("Fetching a single row from data table {}, column {} in order to determine whether data is in GZIP format", dataTable, column);
        try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
            ResultSet rs = conn.createStatement().executeQuery(String.format("SELECT %s FROM %s WHERE %s IS NOT NULL LIMIT 1", column, dataTable, column));
            while (rs.next()) {
                byte[] xmiData = rs.getBytes(column);
                if (xmiData == null)
                    throw new IllegalArgumentException("No data was retrieved from the given column.");
                try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(xmiData))) {
                    useBinaryFormat = BinaryJedisFormatUtils.checkForJeDISBinaryFormat(gzis);
                } catch (IOException e) {
                    log.debug("Attempt to read XMI data in GZIP format failed. Assuming non-gzipped XMI data.");
                    doGzip = false;
                    useBinaryFormat = BinaryJedisFormatUtils.checkForJeDISBinaryFormat(xmiData);
                }
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("does not exist"))
                log.error("An exception occurred when trying to read the xmi column of the data table \"{}\". It seems the table does not contain XMI data and this is invalid to use with this reader.", dataTable);
            throw new CoStoSysException(e);
        }
        log.debug("Found gzip: {}, binary: {}", doGzip, useBinaryFormat);
        return new JedisDataFormat(doGzip, useBinaryFormat);
    }

    public static Map<Integer, String> getReverseBinaryMappingFromDb(DataBaseConnector dbc, String xmiMetaSchema) {
        Map<Integer, String> map = null;
        final String mappingTableName = xmiMetaSchema + "." + XmiSplitConstants.BINARY_MAPPING_TABLE;
        if (dbc.tableExists(mappingTableName)) {
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                conn.setAutoCommit(true);
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.BINARY_MAPPING_COL_ID, XmiSplitConstants.BINARY_MAPPING_COL_STRING,
                        mappingTableName);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getInt(1), rs.getString(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            log.warn(
                    "JeDIS XMI annotation module meta table \"{}\" was not found. It is assumed that the table from which is read contains complete XMI documents.",
                    xmiMetaSchema + "." + XmiSplitConstants.BINARY_MAPPING_TABLE);
        }
        return map;
    }

    public static Map<String, Boolean> getFeaturesToMapBinaryFromDb(DataBaseConnector dbc, String xmiMetaSchema) {
        Map<String, Boolean> map = null;
        final String mappingTableName = xmiMetaSchema + "." + XmiSplitConstants.BINARY_FEATURES_TO_MAP_TABLE;
        if (dbc.tableExists(mappingTableName)) {
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                conn.setAutoCommit(true);
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.BINARY_FEATURES_TO_MAP_COL_FEATURE, XmiSplitConstants.BINARY_FEATURES_TO_MAP_COL_MAP,
                        mappingTableName);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getString(1), rs.getBoolean(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            log.warn(
                    "JeDIS XMI annotation module meta table \"{}\" was not found. It is assumed that the table from which is read contains complete XMI documents.",
                    xmiMetaSchema + "." + XmiSplitConstants.BINARY_FEATURES_TO_MAP_TABLE);
        }
        return map;
    }

    public static Map<String, String> getNamespaceMap(DataBaseConnector dbc, String xmiMetaSchema) {
        Map<String, String> map = null;
        if (dbc.tableExists(xmiMetaSchema + "." + XmiSplitConstants.XMI_NS_TABLE)) {
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                conn.setAutoCommit(true);
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.PREFIX, XmiSplitConstants.NS_URI,
                        xmiMetaSchema + "." + XmiSplitConstants.XMI_NS_TABLE);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getString(1), rs.getString(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            log.warn(
                    "Table \"{}\" was not found it is assumed that the table from which is read contains complete XMI documents.",
                    dbc.getActiveDataPGSchema() + "." + XmiSplitConstants.XMI_NS_TABLE);
        }
        return map;
    }

    public static class JedisDataFormat {
        private boolean gzip;
        private boolean binary;

        public JedisDataFormat(boolean gzip, boolean binary) {
            this.gzip = gzip;
            this.binary = binary;
        }

        public boolean isGzip() {
            return gzip;
        }

        public boolean isBinary() {
            return binary;
        }
    }
}
