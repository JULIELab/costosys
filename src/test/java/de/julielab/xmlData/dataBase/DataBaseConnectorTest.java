package de.julielab.xmlData.dataBase;

import de.julielab.xmlData.Constants;
import de.julielab.xmlData.cli.TableNotFoundException;
import de.julielab.xmlData.dataBase.util.TableSchemaMismatchException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

public class DataBaseConnectorTest {

    @ClassRule
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer();
    private static DataBaseConnector dbc;

    @BeforeClass
    public static void setup() throws SQLException, IOException {
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
    }

    @Test
    public void testRetrieveAndMark() throws SQLException, TableSchemaMismatchException {
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
    }

    // Depends on the test above!
    @Test
    public void testStatus() throws SQLException, TableSchemaMismatchException, TableNotFoundException {
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
    }

    // Depends on the test above!
    @Test
    public void testRandomSubset() throws SQLException {
        dbc.createSubsetTable("randomsubset", Constants.DEFAULT_DATA_TABLE_NAME, "Random Test Subset");
        dbc.initRandomSubset(10, "randomsubset", Constants.DEFAULT_DATA_TABLE_NAME);
        Connection conn = dbc.getConn();
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM randomsubset");
        int numrows = 0;
        while (rs.next()) {
            numrows++;
        }
        assertThat(numrows).isEqualTo(10);
        conn.close();
    }
}
