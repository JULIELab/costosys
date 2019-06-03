package de.julielab.costosys.cli;


import de.julielab.costosys.cli.CLI;
import de.julielab.costosys.dbconnection.SubsetStatus;
import de.julielab.jcore.db.test.DBTestUtils;
import de.julielab.costosys.Constants;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.*;
import static org.testng.AssertJUnit.assertEquals;

public class CLITest {

    public static PostgreSQLContainer postgres;
    private static DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        postgres =  new PostgreSQLContainer();
        postgres.start();
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2017");

        String configurationPath = DBTestUtils.createTestCostosysConfig("medline_2017", 1, postgres);
        System.setProperty(Constants.COSTOSYS_CONFIG_FILE, configurationPath);
        DBTestUtils.createAndSetHiddenConfig("src/test/resources/hiddenconfig.txt", postgres);
    }

    @AfterClass
    public static void shutdown(){
        dbc.close();
        postgres.stop();
    }

    @Test
    public void testImport()  {
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-i", "src/test/resources/pubmedsample18n0001.xml.gz"})).doesNotThrowAnyException();
        dbc.reserveConnection();
        assertThat(dbc.tableExists(Constants.DEFAULT_DATA_TABLE_NAME));
        assertThat(dbc.getNumRows(Constants.DEFAULT_DATA_TABLE_NAME)).isEqualTo(177);
    }

    @Test(dependsOnMethods = "testImport")
    public void testCreateSubset() {
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-s", "all_subset", "-a"})).doesNotThrowAnyException();
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-s", "random_subset", "-r", "10"})).doesNotThrowAnyException();
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-s", "mirror_subset", "-m"})).doesNotThrowAnyException();
    }

    @Test(dependsOnMethods = "testCreateSubset")
    public void testStatus() {
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-st", "all_subset"})).doesNotThrowAnyException();
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-st", "random_subset"})).doesNotThrowAnyException();
        assertThatCode(() -> CLI.main(new String[]{"-st", "mirror_subset"})).doesNotThrowAnyException();
    }

    @Test(dependsOnMethods = "testImport")
    public void testQueryDocuments() {
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-q", "-z", "all_subset"})).doesNotThrowAnyException();
    }

    @Test(dependsOnMethods = {"testImport", "testCreateSubset"})
    public void testMarkAsProcessed() throws Exception {
        final SubsetStatus processedBefore = dbc.status("all_subset", EnumSet.of(DataBaseConnector.StatusElement.IS_PROCESSED));
        assertEquals(0L, (long)processedBefore.isProcessed);
        assertThatCode(() -> de.julielab.costosys.cli.CLI.main(new String[]{"-mp", "all_subset", "-f", Path.of("src", "test", "resources", "markAsProcessedTestIds.txt").toString()})).doesNotThrowAnyException();
        final SubsetStatus processedAfter = dbc.status("all_subset", EnumSet.of(DataBaseConnector.StatusElement.IS_PROCESSED));
        assertEquals(2L, (long)processedAfter.isProcessed);
    }
}
