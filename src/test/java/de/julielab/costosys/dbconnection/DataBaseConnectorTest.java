package de.julielab.costosys.dbconnection;

import de.julielab.costosys.Constants;
import de.julielab.costosys.cli.TableNotFoundException;
import de.julielab.costosys.configuration.FieldConfig;
import de.julielab.costosys.dbconnection.util.TableSchemaMismatchException;
import de.julielab.java.utilities.IOStreamUtilities;
import de.julielab.xml.JulieXMLConstants;
import org.postgresql.jdbc.PgSQLXML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

public class DataBaseConnectorTest {
    private final static Logger log = LoggerFactory.getLogger(DataBaseConnectorTest.class);
    public static PostgreSQLContainer postgres;
    private static DataBaseConnector dbc;

    @BeforeClass
    public static void setUp() {
        postgres = new PostgreSQLContainer<>("postgres:" + DataBaseConnector.POSTGRES_VERSION);
        postgres.start();
        dbc = new DataBaseConnector(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        dbc.setActiveTableSchema("medline_2016");
        dbc.setMaxConnections(2);
    }

    @AfterClass
    public static void shutdown() {
        dbc.close();
        postgres.stop();
        log.info("There are {} reserved connections.", dbc.getNumReservedConnections(false));
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
        dbc.reserveConnection(true);
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
    public void testJoinTablesWithDataTable() throws Exception {
        // Copy the medline_2016 field configuration to create a new configuration with the same fields but without setting the primary key for retrieve and not zipping the
        // XML field for simplicity of the test. It is important to create new Maps for the fields because otherwise we would override the original field configuration.
        List<Map<String, String>> additionalTableConfigFields = dbc.getFieldConfiguration("medline_2016").getFields().stream().map(LinkedHashMap::new).collect(Collectors.toList());
        additionalTableConfigFields.get(0).put(JulieXMLConstants.RETRIEVE, "false");
        additionalTableConfigFields.get(1).put(JulieXMLConstants.GZIP, "false");
        final FieldConfig additionalFieldConfig = new FieldConfig(additionalTableConfigFields, "", "medline_2016_additional");
        dbc.addFieldConfiguration(additionalFieldConfig);
        // Create two new tables with some dummy values for each row in the test data tables. We will then join those values
        // when retrieving data from the data table.
        try (CoStoSysConnection costoConn = dbc.obtainOrReserveConnection(true)) {
            assertThat(costoConn.getAutoCommit()).isTrue();
            dbc.resetSubset("testsubset");
            final List<Object[]> pksInTable = dbc.retrieveAndMark("testsubset", "testJoinTablesWithDataTable", "testhost", "0");
            final Statement stmt = costoConn.createStatement();
            dbc.createTable("additionalTable", "A test table for tests with joining to other tables.");
            for (int i = 0; i < pksInTable.size(); i++) {
                Object[] pk = pksInTable.get(i);
                String sql = String.format("INSERT INTO %s VALUES('%s','%s')", "additionalTable", pk[0], "Value" + i);
                stmt.execute(sql);
            }
            dbc.createTable("additionalTable2", "Another test table for tests with joining to other tables.");
            assertThat(costoConn.getAutoCommit()).isTrue();
            for (int i = 0; i < pksInTable.size(); i++) {
                Object[] pk = pksInTable.get(i);
                String sql = String.format("INSERT INTO %s VALUES('%s','%s')", "additionalTable2", pk[0], "Value" + (i + 42));
                stmt.execute(sql);
            }
        }
        final DBCIterator<byte[][]> data = dbc.queryDataTable("_data._data", null, new String[]{"additionalTable", "additionalTable2"}, new String[]{"medline_2016", "medline_2016_additional", "medline_2016_additional"});
        int i = 0;
        while (data.hasNext()) {
            byte[][] joinedData = data.next();
            assertEquals(4, joinedData.length);
            assertThat(new String(joinedData[0], StandardCharsets.UTF_8).matches("[0-9]+"));
            assertThat(new String(joinedData[1], StandardCharsets.UTF_8)).startsWith("<MedlineCitation");
            assertThat(new String(joinedData[2], StandardCharsets.UTF_8)).isEqualTo("Value" + i);
            assertThat(new String(joinedData[3], StandardCharsets.UTF_8)).isEqualTo("Value" + (i++ + 42));
        }
    }

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testStatus() throws SQLException, TableSchemaMismatchException, TableNotFoundException {
        dbc.reserveConnection(true);
        dbc.createSubsetTable("statussubset", Constants.DEFAULT_DATA_TABLE_NAME, "Test subset");
        dbc.initSubset("statussubset", Constants.DEFAULT_DATA_TABLE_NAME);
        // mark a few documents to be in process
        dbc.retrieveAndMark("statussubset", "testcomponent", "localhost", "0", 2, null);
        SubsetStatus status = dbc.status("statussubset", EnumSet.allOf(DataBaseConnector.StatusElement.class));
        assertThat(status.total).isEqualTo(10);
        assertThat(status.inProcess).isEqualTo(2);
        assertThat(status.pipelineStates).containsKeys("testcomponent").extracting("testcomponent").isEqualTo(2L);
        dbc.setQueryBatchSize(2);
        dbc.releaseConnections();
    }

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testRandomSubset() throws SQLException {
        try (CoStoSysConnection conn = dbc.reserveConnection(true)) {
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
        dbc.reserveConnection(true);
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

    @Test(dependsOnMethods = "testRetrieveAndMark")
    public void testMirrorSubset() throws SQLException {
        CoStoSysConnection conn = dbc.reserveConnection(true);
        dbc.defineMirrorSubset("testmirror", Constants.DEFAULT_DATA_TABLE_NAME, true, "A test mirror table");
        assertThat(dbc.getNumRows("testmirror")).isGreaterThan(0);
        Map<String, Boolean> mirrorSubsetNames = dbc.getMirrorSubsetNames(conn, Constants.DEFAULT_DATA_TABLE_NAME);
        assertThat(mirrorSubsetNames.keySet()).contains("public.testmirror");
//        dbc.releaseConnections();
        dbc.dropTable("public.testmirror");
        mirrorSubsetNames = dbc.getMirrorSubsetNames(conn, Constants.DEFAULT_DATA_TABLE_NAME);
        assertThat(mirrorSubsetNames.keySet()).doesNotContain("public.testmirror", "testmirror");
        dbc.releaseConnections();
    }

    @Test
    public void testXmlData() {
        dbc.withConnectionExecute(c -> c.createTable("myxmltest", "xmi_text_legacy", "XML Test Table"));
        Map<String, Object> row = new HashMap<>();
        row.put("docid", "doc1");
        row.put("xmi", "some nonsense");
        dbc.reserveConnection(true);
        assertThatCode(() -> dbc.importFromRowIterator(Arrays.asList(row).iterator(), "myxmltest", "xmi_text_legacy")).doesNotThrowAnyException();
        dbc.releaseConnections();
        // Iterators use their own connection
        DBCIterator<byte[][]> dbcIterator = dbc.queryDataTable("myxmltest", null, null, "xmi_text_legacy");
        byte[][] next = dbcIterator.next();
        assertThat(new String(next[0], StandardCharsets.UTF_8)).isEqualTo("doc1");
        assertThat(new String(next[1], StandardCharsets.UTF_8)).isEqualTo("some nonsense");
    }

    @Test
    public void testGetColumnMetaInformation() {
        final Map<String, String> f1 = FieldConfig.createField(JulieXMLConstants.NAME, "testfield1", JulieXMLConstants.TYPE, "xml");
        final Map<String, String> f2 = FieldConfig.createField(JulieXMLConstants.NAME, "testfield2", JulieXMLConstants.TYPE, "bytea");
        final FieldConfig config = dbc.addXmiTextFieldConfiguration(dbc.getFieldConfiguration("xmi_text_legacy").getPrimaryKeyFields().collect(Collectors.toList()), Arrays.asList(f1, f2), false);
        final List<Map<String, String>> configFields = config.getFields();
        assertEquals(configFields.get(configFields.size() - 2).get(JulieXMLConstants.NAME), "testfield1");
        assertEquals(configFields.get(configFields.size() - 1).get(JulieXMLConstants.NAME), "testfield2");

        dbc.createTable("MyCustomTable", config.getName(), "Created with a custom field configuration.");
        assertThat(dbc.withConnectionQueryBoolean(dbc -> dbc.tableExists("MyCustomTable"))).isTrue();
        assertThat(dbc.withConnectionQueryBoolean(dbc -> dbc.tableExists("mycustomtable"))).isTrue();

        final List<Map<String, Object>> columnInfo = dbc.getTableColumnInformation("MyCustomTable", "column_name", "data_type");
        int colsFound = 0;
        for (Map<String, Object> info : columnInfo) {
            if (info.get("column_name").equals("testfield1")) {
                assertThat(info.get("data_type")).isEqualTo("xml");
                ++colsFound;
            } else if (info.get("column_name").equals("testfield2")) {
                assertThat(info.get("data_type")).isEqualTo("bytea");
                ++colsFound;
            }
        }
        assertThat(colsFound).isEqualTo(2);
    }

    @Test
    public void testAssureColumnsExist() throws Exception {
        dbc.createTable("MyColumnExtensionTable", "medline_2017");
        dbc.assureColumnsExist("MyColumnExtensionTable", Arrays.asList("newCol1", "newCol2"), "xml");

        final List<Map<String, Object>> infos = dbc.getTableColumnInformation("MyColumnExtensionTable", "column_name", "data_type");
        int colsFound = 0;
        for (Map<String, Object> info : infos) {
            if (info.get("column_name").equals("newcol1")) {
                assertThat(info.get("data_type")).isEqualTo("xml");
                ++colsFound;
            } else if (info.get("column_name").equals("newcol2")) {
                assertThat(info.get("data_type")).isEqualTo("xml");
                ++colsFound;
            }
        }
        assertThat(colsFound).isEqualTo(2);
    }

    @Test
    public void testUpdateToNullValue() throws Exception {
        dbc.setActiveTableSchema("pubmed");
        dbc.createTable("TableWithNull", "for tests with null values");
        List<Map<String, Object>> rows = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("pmid", "1234");
        row.put("xml", "<xmi>content1</xmi>");
        rows.add(row);
        row = new HashMap<>();
        row.put("pmid", "5678");
        row.put("xml", "<xmi>content2</xmi>");
        rows.add(row);

        dbc.importFromRowIterator(rows.iterator(), "TableWithNull", true, "pubmed");

        DBCIterator<Object[]> dbcIterator = dbc.query("TableWithNull", Arrays.asList("pmid", "xml"));
        List<String> ids = new ArrayList<>();
        List<String> xml = new ArrayList<>();
        while (dbcIterator.hasNext()) {
            Object[] next = dbcIterator.next();
            ids.add((String) next[0]);
            xml.add(((PgSQLXML) next[1]).getString());
        }
        assertThat(ids).containsExactly("1234", "5678");
        assertThat(xml).containsExactly("<xmi>content1</xmi>", "<xmi>content2</xmi>");

        rows.clear();
        row = new HashMap<>();
        row.put("pmid", "5678");
        row.put("xml", null);
        rows.add(row);

        dbc.updateFromRowIterator(rows.iterator(), "TableWithNull", true, true, "pubmed");
        dbcIterator = dbc.query("TableWithNull", Arrays.asList("pmid", "xml"));
        ids = new ArrayList<>();
        xml = new ArrayList<>();
        while (dbcIterator.hasNext()) {
            Object[] next = dbcIterator.next();
            ids.add((String) next[0]);
            xml.add(next[1] != null ? ((PgSQLXML) next[1]).getString() : (String) next[1]);
        }
        assertThat(ids).containsExactly("1234", "5678");
        assertThat(xml).containsExactly("<xmi>content1</xmi>", null);
    }

    @Test
    public void testUpdate() throws Exception {
        // Insert the test data, delete half of it, change the other half, update from the original data, check
        // that everything is as it should be according to the original XML data.
        CoStoSysConnection conn = dbc.reserveConnection(true);
        String dataTableName = "update_test";
        dbc.createTable(dataTableName, "Test data table");
        dbc.importFromXMLFile("src/test/resources/documents/documentSet.xml.gz", dataTableName);
        assertThat(dbc.getNumRows(dataTableName)).isEqualTo(10);
        // Preparation: Retrieve the document IDs and delete half.
        String primaryKeyString = dbc.getActiveTableFieldConfiguration().getPrimaryKeyString();
        List<String> insertedDocumentIds = new ArrayList<>();
        ResultSet rs = conn.createStatement().executeQuery("SELECT " + primaryKeyString + " FROM " + dataTableName);
        while (rs.next())
            insertedDocumentIds.add(rs.getString(1));
        List<String> toDelete = insertedDocumentIds.subList(5, 10);
        PreparedStatement ps = conn.prepareStatement("DELETE FROM " + dataTableName + " WHERE " + primaryKeyString + "=?");
        for (int i = 0; i < toDelete.size(); i++) {
            String docIdToDelete = toDelete.get(i);
            ps.setString(1, docIdToDelete);
            ps.addBatch();
        }
        ps.executeBatch();
        assertThat(dbc.getNumRows(dataTableName)).isEqualTo(5);

        // Peparation: And now also change the XML field contents of the first row
        String testValue = "<sometag>testvalue</sometag>";
        conn.createStatement().execute("UPDATE " + dataTableName + " SET xml='" + testValue + "' WHERE " + primaryKeyString + "='" + insertedDocumentIds.get(0) + "'");
        ResultSet rs2 = conn.createStatement().executeQuery("SELECT xml FROM " + dataTableName + " WHERE " + primaryKeyString + "='" + insertedDocumentIds.get(0) + "'");
        assertTrue(rs2.next());
        assertThat(new String(rs2.getBytes(1), StandardCharsets.UTF_8)).isEqualTo(testValue);

        // The actual test: Update from the original XML and make sure everything is in order
        dbc.updateFromXML("src/test/resources/documents/documentSet.xml.gz", dataTableName, false);
        // Check that missing documents have been added
        assertThat(dbc.getNumRows(dataTableName)).isEqualTo(10);
        // Check that existing documents have been updated
        ResultSet rs3 = conn.createStatement().executeQuery("SELECT xml FROM " + dataTableName + " WHERE " + primaryKeyString + "='" + insertedDocumentIds.get(0) + "'");
        assertTrue(rs3.next());
        String xmlValue;
        try (ByteArrayInputStream xmlBytes = new ByteArrayInputStream(rs3.getBytes(1)); BufferedInputStream bis = new BufferedInputStream(new GZIPInputStream(xmlBytes))) {
            xmlValue = IOStreamUtilities.getStringFromInputStream(bis);
        }
        assertThat(xmlValue).startsWith("<MedlineCitation Owner=\"NLM\" Status=\"MEDLINE\">").contains("<PMID Version=\"1\">10922238</PMID>").endsWith("<NumberOfReferences>25</NumberOfReferences>\n" +
                "</MedlineCitation>");

        dbc.dropTable(dataTableName);
        dbc.releaseConnections();
    }


    @Test
    public void testColumnEmpty() {
        dbc.createTable("EmptyTable", "medline_2017", "Just an empty table");
        assertTrue(dbc.isEmpty("EmptyTable", "xml"));
    }

    @Test
    public void testmuh() throws Exception {
        Connection conn = dbc.getConn();
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        boolean execute = s.execute("CREATE TABLE mymuhtest (id text PRIMARY KEY, text text)");
        System.out.println("Table creation: " + execute);
        boolean execute1 = s.execute("INSERT INTO mymuhtest VALUES ('hallo', 'der text')");
        System.out.println("Row insertion: " + execute1);
        ResultSet rs = s.executeQuery("SELECT * FROM mymuhtest");
        System.out.println("Table contents:");
        while (rs.next()) {
            System.out.println(rs.getString(1) + ", " + rs.getString(2));
        }
        // TODO das hier loest einen commit aus! In DataBaseConnection#2546 wird also ggfs ungewollt commited
        conn.setAutoCommit(true);
        conn.close();


        conn = dbc.getConn();
        s = conn.createStatement();
        rs = s.executeQuery("SELECT * FROM mymuhtest");
        System.out.println("Table contents with new connection:");
        while (rs.next()) {
            System.out.println(rs.getString(1) + ", " + rs.getString(2));
        }
        conn.close();
    }

}
