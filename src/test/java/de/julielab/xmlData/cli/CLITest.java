package de.julielab.xmlData.cli;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.sql.SQLException;

import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Ignore;

import de.julielab.xmlData.DBUnitBaseTestClass;

/**
 * 
 * @author faessler
 * 
 */
@Ignore
public class CLITest extends DBUnitBaseTestClass {

	/**
	 * @param name
	 * @throws Exception
	 */
	public CLITest(String name) throws Exception {
		super(name);
		// necessary to set the correct path of the configuration file the CLI
		// loads.
		try {
			Field field = CLI.class.getDeclaredField("USER_SCHEME_DEFINITION");
			field.setAccessible(true);
			field.set(null, "src/test/resources/DBTest/testConfiguration.xml");
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Executed to load data for tests into table
	 */
	@Override
	protected IDataSet getDataSet() throws Exception {
		return new FlatXmlDataSetBuilder().build(new FileInputStream(DATA));
	}

	/**
	 * The table will be deleted and repopulated for every test
	 */
	@Override
	protected DatabaseOperation getSetUpOperation() throws Exception {
		return DatabaseOperation.CLEAN_INSERT;
	}

	//
	// /**
	// * The table will be deleted after every test (an thus is empty after the
	// * last)
	// */
	// @Override
	// protected DatabaseOperation getTearDownOperation() throws Exception {
	// return DatabaseOperation.DELETE_ALL;
	// // return DatabaseOperation.NONE;
	// }

	public void testCLIimport() throws DatabaseUnitException, SQLException,
			Exception {
		// For this particular test we want an empty data table.
		DatabaseOperation.DELETE_ALL.execute(getConnection(), testDataSet);

		// Do the import.
		CLI.main(new String[] { "-v", "-i", IMPORT});

		// Check whether the import was successful.
		IDataSet dataSet = getConnection().createDataSet(
				new String[] { TABLE_DATA });

		Assertion.assertEquals(testDataSet.getTable(TABLE_DATA),
				dataSet.getTable(TABLE_DATA));
	}

	// -f, -o, -a, -w

	public void testCLISubsetR() throws DataSetException, SQLException,
			Exception {
		getConnection().getConnection().createStatement().execute("DROP TABLE " + SCHEMA + "." + TABLE_SUBSET);
		CLI.main(new String[] { "-s", TABLE_SUBSET, "-r", "1" });
		assertEquals(1, getConnection().createTable(SCHEMA + "." + TABLE_SUBSET).getRowCount());
	}

	/* (non-Javadoc)
	 * @see de.julielab.xmlData.DBUnitBaseTestClass#getDBSchema()
	 */
	@Override
	protected String getDBSchema() {
		return "database_connector";
	}
	
	@Override
	protected String getDBCConfiguration() {
		return "src/test/resources/DBTest/testConfiguration.xml";
	}

	@Override
	protected String getHiddenConfigPath() {
		return "src/test/resources/DBTest/hiddenConfig";
	}

}
