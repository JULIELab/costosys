package de.julielab.costosys.dbconnection;

import de.julielab.costosys.Constants;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ThreadedColumnsToRetrieveIteratorTest {
    public static PostgreSQLContainer postgres;
    private static de.julielab.costosys.dbconnection.DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() throws SQLException {
        postgres = new PostgreSQLContainer<>("postgres:11.12");
        postgres.start();
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
        dbc.reserveConnection();
        dbc.createTable(Constants.DEFAULT_DATA_TABLE_NAME, "Test data table");
        dbc.importFromXMLFile("src/test/resources/documents/documentSet.xml.gz", Constants.DEFAULT_DATA_TABLE_NAME);
        assertEquals(10, dbc.getNumRows(Constants.DEFAULT_DATA_TABLE_NAME));
        dbc.setQueryBatchSize(3);
        dbc.releaseConnections();
    }

    @AfterClass
    public static void shutDown() {
        dbc.close();
    }

    @Test
    public void testIterator() {
        try (CoStoSysConnection conn = dbc.reserveConnection()) {
            de.julielab.costosys.dbconnection.ThreadedColumnsToRetrieveIterator it = new de.julielab.costosys.dbconnection.ThreadedColumnsToRetrieveIterator(dbc, conn, Arrays.<Object[]>asList(new Object[]{"10922238"}), Constants.DEFAULT_DATA_TABLE_NAME, "medline_2016");
            int numRetrieved = 0;
            while (it.hasNext()) {
                Object[] next = it.next();
                Arrays.toString(next);
                numRetrieved++;
            }
            assertEquals(1, numRetrieved);
        }
    }

    @Test
    public void testIteratorWithoutExternalConnection() {
        de.julielab.costosys.dbconnection.ThreadedColumnsToRetrieveIterator it = new ThreadedColumnsToRetrieveIterator(dbc, null, Arrays.<Object[]>asList(new Object[]{"10922238"}), Constants.DEFAULT_DATA_TABLE_NAME, "medline_2016");
        int numRetrieved = 0;
        while (it.hasNext()) {
            Object[] next = it.next();
            Arrays.toString(next);
            numRetrieved++;
        }
        assertEquals(1, numRetrieved);
    }


}
