package de.julielab.costosys.dbconnection;

import de.julielab.costosys.Constants;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ThreadedColumnsIteratorTest {
    public static PostgreSQLContainer postgres;
    private static de.julielab.costosys.dbconnection.DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() throws SQLException {
        postgres = new PostgreSQLContainer<>("postgres:11.12");
        postgres.start();
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
        dbc.reserveConnection(true);
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
        try (CoStoSysConnection conn = dbc.reserveConnection(true)) {
            de.julielab.costosys.dbconnection.ThreadedColumnsIterator it = new de.julielab.costosys.dbconnection.ThreadedColumnsIterator(dbc, conn, Arrays.asList("pmid", "xml"), Constants.DEFAULT_DATA_TABLE_NAME);
            int numRetrieved = 0;
            while (it.hasNext()) {
                Object[] next = it.next();
                Arrays.toString(next);
                numRetrieved++;
            }
            assertEquals(10, numRetrieved);
        }
    }

    @Test
    public void testIteratorWithoutExternalConnection() throws InterruptedException {
        // Repeat the very same lines of code a few times to make sure that connections are released properly
        de.julielab.costosys.dbconnection.ThreadedColumnsIterator it = new de.julielab.costosys.dbconnection.ThreadedColumnsIterator(dbc, Arrays.asList("pmid", "xml"), Constants.DEFAULT_DATA_TABLE_NAME);
        int numRetrieved = 0;
        while (it.hasNext()) {
            Object[] next = it.next();
            Arrays.toString(next);
            numRetrieved++;
        }
        assertEquals(10, numRetrieved);

        it = new de.julielab.costosys.dbconnection.ThreadedColumnsIterator(dbc, Arrays.asList("pmid", "xml"), Constants.DEFAULT_DATA_TABLE_NAME);
        numRetrieved = 0;
        while (it.hasNext()) {
            Object[] next = it.next();
            Arrays.toString(next);
            numRetrieved++;
        }
        assertEquals(10, numRetrieved);

        it = new de.julielab.costosys.dbconnection.ThreadedColumnsIterator(dbc, Arrays.asList("pmid", "xml"), Constants.DEFAULT_DATA_TABLE_NAME);
        numRetrieved = 0;
        while (it.hasNext()) {
            Object[] next = it.next();
            Arrays.toString(next);
            numRetrieved++;
        }
        assertEquals(10, numRetrieved);
        it.join();
        assertEquals(0, dbc.getNumReservedConnections(false));
    }

    @Test
    public void testIteratorWithLimit() {
        try (CoStoSysConnection conn = dbc.reserveConnection(true)) {
            de.julielab.costosys.dbconnection.ThreadedColumnsIterator it = new ThreadedColumnsIterator(dbc, conn, Arrays.asList("pmid", "xml"), Constants.DEFAULT_DATA_TABLE_NAME, 2);
            int numRetrieved = 0;
            while (it.hasNext()) {
                Object[] next = it.next();
                Arrays.toString(next);
                numRetrieved++;
            }
            assertEquals(2, numRetrieved);
        }
    }
}
