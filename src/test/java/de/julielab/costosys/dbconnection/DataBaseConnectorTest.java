package de.julielab.costosys.dbconnection;

import de.julielab.costosys.Constants;
import de.julielab.costosys.cli.TableNotFoundException;
import de.julielab.costosys.dbconnection.util.TableSchemaMismatchException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testng.AssertJUnit.assertEquals;

public class DataBaseConnectorTest {

    public static PostgreSQLContainer postgres;
    private static de.julielab.costosys.dbconnection.DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() {
        postgres = new PostgreSQLContainer();
        postgres.start();
        dbc = new de.julielab.costosys.dbconnection.DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
    }

    @AfterClass
    public static void shutdown() {
        postgres.stop();
        dbc.close();
    }


    @Test
    public void testQueryAndExecution() {
        dbc.withConnectionExecute(dbc -> {
            try {
                dbc.createTable("mytable", "Some comment");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        boolean exists = dbc.withConnectionQueryBoolean(dbc -> dbc.tableExists("mytable"));
        assertThat(exists).isTrue();
    }

    @Test
    public void testRetrieveAndMark() throws Exception {
        dbc.reserveConnection();
        dbc.createTable(Constants.DEFAULT_DATA_TABLE_NAME, "Test data table");
        dbc.importFromXMLFile("src/test/resources/documents/documentSet.xml.gz", Constants.DEFAULT_DATA_TABLE_NAME);
        dbc.createSubsetTable("testsubset", Constants.DEFAULT_DATA_TABLE_NAME, "Test subset");
        dbc.initSubset("testsubset", Constants.DEFAULT_DATA_TABLE_NAME);
        assertEquals(10, dbc.getNumRows("testsubset"));
        for (int i = 0; i < 10; i += 2) {
            List<Object[]> retrievedKeys = dbc.retrieveAndMark("testsubset", "unit-test", "localhost", "1", 2, null);
            assertEquals(2, retrievedKeys.size());
        }
        List<Object[]> retrievedKeys = dbc.retrieveAndMark("testsubset", "unit-test", "localhost", "1", 2, null);
        assertEquals(0, retrievedKeys.size());
        dbc.releaseConnections();
    }

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testStatus() throws SQLException, TableSchemaMismatchException, TableNotFoundException {
        dbc.reserveConnection();
        dbc.createSubsetTable("statussubset", Constants.DEFAULT_DATA_TABLE_NAME, "Test subset");
        dbc.initSubset("statussubset", Constants.DEFAULT_DATA_TABLE_NAME);
        int bs = dbc.getQueryBatchSize();
        // mark a few documents to be in process
        dbc.retrieveAndMark("statussubset", "testcomponent", "localhost", "0", 2, null);
        SubsetStatus status = dbc.status("statussubset", EnumSet.allOf(DataBaseConnector.StatusElement.class));
        assertThat(status.total).isEqualTo(10);
        assertThat(status.inProcess).isEqualTo(2);
        assertThat(status.pipelineStates).containsKeys("testcomponent").extracting("testcomponent").contains(2L);
        dbc.setQueryBatchSize(2);
        dbc.releaseConnections();
    }

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testRandomSubset() throws SQLException {
        try (CoStoSysConnection conn = dbc.reserveConnection()) {
            dbc.createSubsetTable("randomsubset", Constants.DEFAULT_DATA_TABLE_NAME, "Random Test Subset");
            dbc.initRandomSubset(10, "randomsubset", Constants.DEFAULT_DATA_TABLE_NAME);
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM randomsubset");
            int numrows = 0;
            while (rs.next()) {
                numrows++;
            }
            assertThat(numrows).isEqualTo(10);
        }
    }

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testQuerySubset() throws SQLException {
        dbc.reserveConnection();
        dbc.createSubsetTable("querysubset", Constants.DEFAULT_DATA_TABLE_NAME, "");
        dbc.initSubset("querysubset", Constants.DEFAULT_DATA_TABLE_NAME);
        assertThat(dbc.getNumRows("querysubset")).isGreaterThan(0);
        dbc.releaseConnections();
        de.julielab.costosys.dbconnection.DBCIterator<byte[][]> it = dbc.querySubset("querysubset", 0);
        Set<String> retrieved = new HashSet<>();
        while (it.hasNext()) {
            byte[][] next = it.next();
            retrieved.add(new String(next[0]));
        }
        assertThat(retrieved).hasSize(10);
    }

    @Test
    public void testXmlData() throws UnsupportedEncodingException {
        dbc.withConnectionExecute(c -> c.createTable("myxmltest", "xmi_text", "XML Test Table"));
        Map<String, Object> row = new HashMap<>();
        row.put("docid", "doc1");
        row.put("xmi", "some nonsense");
        dbc.reserveConnection();
        assertThatCode(() -> dbc.importFromRowIterator(Arrays.asList(row).iterator(), "myxmltest", "xmi_text")).doesNotThrowAnyException();
        dbc.releaseConnections();
        // Iterators use their own connection
        DBCIterator<byte[][]> dbcIterator = dbc.queryDataTable("myxmltest", null, "xmi_text");
        byte[][] next = dbcIterator.next();
        assertThat(new String(next[0], "UTF-8")).isEqualTo("doc1");
        assertThat(new String(next[1], "UTF-8")).isEqualTo("some nonsense");
    }


}
