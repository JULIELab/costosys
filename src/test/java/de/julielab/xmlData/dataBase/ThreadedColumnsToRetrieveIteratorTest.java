package de.julielab.xmlData.dataBase;

import de.julielab.xmlData.Constants;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ThreadedColumnsToRetrieveIteratorTest {
    private final static Logger log = LoggerFactory.getLogger(ThreadedColumnsToRetrieveIteratorTest.class);
    @ClassRule
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer();
    private static DataBaseConnector dbc;

    @BeforeClass
    public static void setup() throws SQLException, IOException {
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
    public void testIterator() throws Exception {
        try (CoStoSysConnection conn = dbc.reserveConnection()) {
            ThreadedColumnsToRetrieveIterator it = new ThreadedColumnsToRetrieveIterator(dbc, conn, Arrays.<Object[]>asList(new Object[]{"10922238"}), Constants.DEFAULT_DATA_TABLE_NAME, "medline_2016");
            int numRetrieved = 0;
            while (it.hasNext()) {
                Object[] next = it.next();
                Arrays.toString(next);
                numRetrieved++;
            }
            assertEquals(1, numRetrieved);
        }
    }


}
