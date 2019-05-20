package de.julielab.costosys.dbconnection;

import de.julielab.costosys.Constants;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ThreadedColumnsIteratorTest {
    private final static Logger log = LoggerFactory.getLogger(ThreadedColumnsIteratorTest.class);
    @ClassRule
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer();
    private static de.julielab.costosys.dbconnection.DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() throws SQLException, IOException {
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
        dbc.reserveConnection();
        dbc.createTable(Constants.DEFAULT_DATA_TABLE_NAME, "Test data table");
        dbc.importFromXMLFile("src/test/resources/documents/documentSet.xml.gz", Constants.DEFAULT_DATA_TABLE_NAME);
        assertEquals(10, dbc.getNumRows(Constants.DEFAULT_DATA_TABLE_NAME));
        dbc.setQueryBatchSize(3);
        dbc.releaseConnections();
    }

    @Test
    public void testIterator() throws SQLException {
        try (CoStoSysConnection conn = dbc.reserveConnection()) {
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
        assertEquals(0, dbc.getNumReservedConnections());
    }

    @Test
    public void testIteratorWithLimit() throws SQLException {
        try (CoStoSysConnection conn = dbc.reserveConnection()) {
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
