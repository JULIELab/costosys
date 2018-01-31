package de.julielab.xmlData.dataRetrieval;

import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.Iterator;

import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ColumnFilterTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Ignore;

import de.julielab.xmlData.Constants;
import de.julielab.xmlData.DBUnitBaseTestClass;

/**
 * <p>
 * DBUnit test case. Pivot method is "getDataSet()" which loads the test data from a formerly created XML file. For each
 * single test, the database is returned to the exact state of this test data! No matter what you do in a particular
 * test, for the next test the state will be restored.
 * </p>
 * <p>
 * Also note that DBUnit does not mean to simulate the actual database but to simplify unit testing on a real database.
 * Thus, a connection to the real database is required.
 * </p>
 * 
 * @author faessler
 * 
 */
@Ignore
public class DataBaseConnectorTest extends DBUnitBaseTestClass {

	private static final String DBC_CONFIG = "src/test/resources/DBTest/testConfiguration.xml";

	/**
	 * @param name
	 * @throws Exception
	 */
	public DataBaseConnectorTest(String name) throws Exception {
		super(name);
	}

	/**
	 * This is our test data, created by "createTestData()" at the end of the class. For new test cases it may be
	 * necessary to create new, extended versions of this data. Use createTestData() for this purpose (e.g. called from
	 * the constructor, there is a commented-out call already there).
	 */
	@Override
	protected IDataSet getDataSet() throws Exception {
		// To deal with null values we employ a ReplacemenetDataSet. The
		// FlatXMLDataSet would just leave null values out.
		ReplacementDataSet replacementDataSet = new ReplacementDataSet(
				new FlatXmlDataSetBuilder().build(new FileInputStream(DATA)));
		replacementDataSet.addReplacementObject("[NULL]", null);
		replacementDataSet.setStrictReplacement(true);
		return replacementDataSet;
	}

	/**
	 * The table will be deleted and repopulated for every test
	 */
	// @Override
	// protected DatabaseOperation getSetUpOperation() throws Exception {
	// return DatabaseOperation.DELETE_ALL;
	// }

	/**
	 * The table will be deleted after every test (an thus is empty after the last)
	 */
	// @Override
	// protected DatabaseOperation getTearDownOperation() throws Exception {
	// return DatabaseOperation.DELETE_ALL;
	// }

	public void testImport() throws DataSetException, SQLException, Exception {

		// For this particular test we want an empty data table.
		DatabaseOperation.DELETE_ALL.execute(getConnection(), testDataSet);

		// Do the import.
		dbc.importFromXMLFile(IMPORT, TABLE_DATA);

		// Check whether the import was successful.
		IDataSet dataSet = getConnection().createDataSet(new String[] { TABLE_DATA });

		Assertion.assertEquals(testDataSet.getTable(TABLE_DATA), dataSet.getTable(TABLE_DATA));

	}

	public void testQueryDataTable() throws DatabaseUnitException, UnsupportedEncodingException {
		DefaultDataSet dataSet = createNewDataSet();

		DefaultTable queriedTable = (DefaultTable) dataSet.getTable(TABLE_DATA);
		Iterator<byte[][]> it = dbc.queryDataTable(TABLE_DATA, null);
		while (it.hasNext()) {
			byte[][] row = it.next();
			// Add the retrieved data into the DBUnit table for comparison.
			queriedTable.addRow();
			queriedTable.setValue(queriedTable.getRowCount() - 1, "pmid", new String(row[0], "UTF-8"));
			queriedTable.setValue(queriedTable.getRowCount() - 1, "xml", new String(row[1], "UTF-8"));
		}

		Assertion.assertEquals(testDataSet.getTable(TABLE_DATA), dataSet.getTable(TABLE_DATA));

	}

	public void testQueryDataTableWithWhere() throws Exception {
		DefaultDataSet dataSet = createNewDataSet();

		DefaultTable queriedTable = (DefaultTable) dataSet.getTable(TABLE_DATA);
		// Restrict returned documents to all documents whose ID contains a
		// zero.
		String whereClause = "WHERE pmid like '%0%'";
		Iterator<byte[][]> it = dbc.queryDataTable(TABLE_DATA, whereClause);
		while (it.hasNext()) {
			byte[][] row = it.next();
			// Add the retrieved data into the DBUnit table for comparison.
			queriedTable.addRow();
			queriedTable.setValue(queriedTable.getRowCount() - 1, "pmid", new String(row[0], "UTF-8"));
			queriedTable.setValue(queriedTable.getRowCount() - 1, "xml", new String(row[1], "UTF-8"));
		}

		QueryDataSet queryDataSet = new QueryDataSet(getConnection());
		queryDataSet.addTable(TABLE_DATA, "SELECT * FROM " + SCHEMA + "." + TABLE_DATA + " " + whereClause);
		Assertion.assertEquals(queryDataSet, dataSet);

	}

	public void testQuerySubsetTable() throws DataSetException, SQLException, Exception {
		// Here we must truncate the subset first.
		// For this particular test we want an empty data table.
		DatabaseOperation.TRUNCATE_TABLE.execute(getConnection(),
				new DefaultDataSet(testDataSet.getTable(TABLE_SUBSET)));

		dbc.initSubset(TABLE_SUBSET, TABLE_DATA);
		IDataSet subsetDataSet = getConnection().createDataSet(new String[] { TABLE_SUBSET });

		// We must go with the ColumnFilterTable because the subset table has a
		// lot of null values. Those are just not stored at all in the XML
		// format. So we must exclude the rows that are null for the assertion
		// to work properly.
		ColumnFilterTable columnFilterTable = new ColumnFilterTable(subsetDataSet.getTable(TABLE_SUBSET),
				new IColumnFilter() {

					@Override
					public boolean accept(String tableName, Column column) {
						String name = column.getColumnName();
						if (!name.equals(Constants.HAS_ERRORS) && !name.equals(Constants.IS_PROCESSED)
								&& !name.equals(Constants.IN_PROCESS) && !name.equals("pmid"))
							return false;
						return true;
					}
				}) {
		};
		Assertion.assertEquals(testDataSet.getTable(TABLE_SUBSET), columnFilterTable);
	}

	public void testCountRowsOfDataTable() {
		int count = dbc.countRowsOfDataTable(TABLE_DATA, null);
		assertEquals(count, 10);
	}

	public void testGetNextDataTable() throws SQLException {
		String secondDataTable = "seconddata";
		String testsubset = "testsubset";
		dropTables(testsubset, secondDataTable);

		dbc.createTable(secondDataTable, TABLE_DATA, "dbc_junit_gzip", "Test");
		dbc.createSubsetTable(testsubset, "seconddata", "test");
		String nextDataTable = dbc.getNextDataTable("testsubset");
		assertEquals("First data table", getDBSchema() + ".seconddata", nextDataTable);
		nextDataTable = dbc.getNextDataTable(nextDataTable);
		assertEquals("Second data table", getDBSchema() + "." + TABLE_DATA, nextDataTable);

		if (dbc.tableExists(testsubset))
			dbc.dropTable(testsubset);
		if (dbc.tableExists(secondDataTable))
			dbc.dropTable(secondDataTable);

		dropTables(testsubset, secondDataTable);
	}

	public void testIsSubsetTable() throws Exception {
		assertFalse(dbc.isSubsetTable(TABLE_DATA));
		assertTrue(dbc.isSubsetTable(TABLE_SUBSET));
	}

//	public void testCheckTableSchema() throws Exception {
//		String dataTable = "seconddata";
//		String subsetTable = "secondsubset";
//		String thirdDataTable = "thirdData";
//
//		dropTables(subsetTable, thirdDataTable, dataTable);
//
//		dbc.createTable(dataTable, "otherSchema", "TestComment");
//		dbc.createSubsetTable(subsetTable, dataTable, 1, "TestComment", "otherSchema");
//		dbc.createTable(thirdDataTable, dataTable, "otherSchema", "TestComment");
//
//		boolean doesNotMatch = false;
//
//		try {
//			dbc.checkTableDefinition(subsetTable, "defaultSchema");
//		} catch (IllegalStateException e) {
//			doesNotMatch = true;
//		}
//
//		assertTrue("Actual table schema and table schema checked against do not match for subset table", doesNotMatch);
//
//		doesNotMatch = false;
//		try {
//			dbc.checkTableDefinition(thirdDataTable, "defaultSchema");
//		} catch (IllegalStateException e) {
//			doesNotMatch = true;
//		}
//		assertTrue("Actual table schema and table schema checked against do not match for data table",
//				doesNotMatch);
//
//		dropTables(subsetTable, thirdDataTable, dataTable);
//
//	}

	public void testCreateSubset() throws Exception {
		String secondDataTable = "seconddata";
		String superSubset = "superSubset";
		String subsetName = "subset_of_testsubset";
		dropTables(subsetName, superSubset, secondDataTable);

		dbc.createTable(secondDataTable, TABLE_DATA, "dbc_junit_gzip", "Test");
		dbc.createSubsetTable(superSubset, secondDataTable, "test");

		dbc.createSubsetTable(subsetName, superSubset, "test");
		String referencedTable = dbc.getReferencedTable(subsetName);
		assertEquals("Referenced table of subset with another subset as superset", getDBSchema() + "."
				+ secondDataTable, referencedTable);

		dbc.dropTable(subsetName);

		dbc.createSubsetTable(subsetName, superSubset, 2, "test", "dbc_junit_gzip");
		referencedTable = dbc.getReferencedTable(subsetName);

		assertEquals("Referenced table of subset with another subset as superset with one data table reference hop",
				getDBSchema() + "." + TABLE_DATA, referencedTable);

		dropTables(subsetName, superSubset, secondDataTable);
	}

	private void dropTables(String... tables) throws SQLException {
		for (String table : tables) {
			if (dbc.tableExists(table))
				dbc.dropTable(table);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.julielab.xmlData.DBUnitBaseTestClass#getDBSchema()
	 */
	@Override
	protected String getDBSchema() {
		return "database_connector";
	}

	@Override
	protected String getDBCConfiguration() {
		return DBC_CONFIG;
	}

	@Override
	protected String getHiddenConfigPath() {
		return "src/test/resources/DBTest/hiddenConfig";
	}

	/**
	 * Sets up a clear database for the unit tests of the DBC.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		DataBaseConnectorTest dbcTest = new DataBaseConnectorTest("main");
		dbcTest.setupTestDatabase();
		dbcTest.createTestDataset();
	}

	/**
	 * Creates tables for the unit tests. This is required because DBUnit doesn't create the tables itself from the test
	 * datasets.
	 * 
	 * @throws Exception
	 */
	private void setupTestDatabase() throws Exception {
		if (!dbc.tableExists(TABLE_DATA))
			dbc.createTable(TABLE_DATA, "Created by unit test.");
		if (!dbc.tableExists(TABLE_SUBSET))
			dbc.createSubsetTable(TABLE_SUBSET, TABLE_DATA, "Created by unit test.");
	}

	/**
	 * Creates the test dataset by creating tables, importing data from the test resources and storing them as an XML
	 * file.
	 * 
	 * @throws Exception
	 */
	private void createTestDataset() throws Exception {
		if (!dbc.tableExists(TABLE_DATA))
			dbc.createTable(TABLE_DATA, "Data table for unit tests.");
		dbc.importFromXMLFile(IMPORT, TABLE_DATA);

		// Create a CPE table for this data table.
		if (!dbc.tableExists(TABLE_SUBSET))
			dbc.createSubsetTable(TABLE_SUBSET, TABLE_DATA, "Subset table for unit tests.");
		dbc.initSubset(TABLE_SUBSET, TABLE_DATA);

		// Store the data.
		IDataSet dataSet = getConnection().createDataSet(new String[] { TABLE_DATA, TABLE_SUBSET });

		writeDataSetToXMLFile(dataSet, "src/test/resources/DBTest/testDataSet.xml");

		// Empty the tables to ready them for tests.
		DatabaseOperation.DELETE_ALL.execute(getConnection(), testDataSet);
	}

}
