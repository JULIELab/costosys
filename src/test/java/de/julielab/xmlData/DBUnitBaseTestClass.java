/**
 * DBUnitBaseTestClass.java
 *
 * Copyright (c) 2012, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 07.01.2012
 **/

/**
 * 
 */
package de.julielab.xmlData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.dbunit.DBTestCase;
import org.dbunit.PropertiesBasedJdbcDatabaseTester;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

import de.julielab.xmlData.dataBase.DataBaseConnector;

/**
 * <p>
 * Superclass for common database testing. This class extends
 * <code>DBUnitTestCase</code> and its constructor automatically creates a
 * <code>DataBaseConnecto</code> instance, loads the test data from XML and
 * creates a connection to the test database.
 * </p>
 * <p>
 * For common test cases which don't rely on specials too far away from the
 * defaults in this class, this class should be extended.
 * </p>
 * 
 * @author faessler
 * 
 */
public abstract class DBUnitBaseTestClass extends DBTestCase {

	public final static String DRIVER = "org.postgresql.Driver";
	/**
	 * The connection must be specified here AND in the test database
	 * configuration file! The information here is for DBUnit where the DBC
	 * configuration file is for the tested code.
	 */
	public final static String DB_URL = "jdbc:postgresql://localhost/unittests";
	public final static String USER = "postgres";
	public final static String PASS = "testpassword";
	public final static String TABLE_DATA = "_data";
	public final static String TABLE_SUBSET = "subset";
	public static final String SCHEMA = "jedis";
	 public static final String CONFIG =
	 "src/test/resources/DBTest/testConfiguration.xml";
	public static final String IMPORT = "src/test/resources/documents";
	public static final String DATA = "src/test/resources/DBTest/testDataSet.xml";

	protected IDataSet testDataSet;
	protected DataBaseConnector dbc;

	public DBUnitBaseTestClass(String name) throws Exception {
		super(name);
		// Properties for the test connection which is returned by
		// getConnection().
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, getDBDriver());
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, getDBUrl());
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, getDBUser());
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, getDBPassword());
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_SCHEMA, getDBSchema());
		// Specification of an alternate saving place for the database logins
		// for test purposes.
		System.setProperty(Constants.HIDDEN_CONFIG_PATH, getHiddenConfigPath());
		dbc = new DataBaseConnector("localhost", "unittests", USER, PASS, SCHEMA, 1, new FileInputStream(CONFIG));
		dbc.setActiveTableSchema("unittest");
		
		testDataSet = getDataSet();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		dbc.close();
	}

	protected String getDBDriver() {
		return DRIVER;
	}

	protected String getDBUrl() {
		return DB_URL;
	}

	protected String getDBUser() {
		return USER;
	}

	protected String getDBPassword() {
		return PASS;
	}

	protected abstract String getDBSchema();

	protected String getDBCConfiguration() {
		return "src/test/resources/dbcConfiguration.xml";
	}

	protected String getHiddenConfigPath() {
		return "src/test/resources/hiddenConfig";
	}

	/**
	 * Changes the type system to postgres's
	 */
	@Override
	protected void setUpDatabaseConfig(DatabaseConfig config) {
		config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
	}

	/**
	 * <p>
	 * Helper method to create a new DBUnit data set with one data table named
	 * <code>_data</code> which can then be filled with data in order to test
	 * this new data set against a test data set.
	 * </p>
	 * 
	 * @return An empty data set with the default <code>pmid</code>,
	 *         <code>xml</code> data scheme.
	 * @throws AmbiguousTableNameException
	 */
	protected DefaultDataSet createNewDataSet() throws AmbiguousTableNameException {
		// Build a DBUnit table. First specify its columns.
		Column[] queriedTableColumns = new Column[] { new Column("pmid", DataType.VARCHAR),
				new Column("xml", DataType.VARCHAR) };
		// Build the table itself.
		DefaultTable queriedTable = new DefaultTable(TABLE_DATA, queriedTableColumns);
		DefaultDataSet dataSet = new DefaultDataSet();
		// Put the table, we will fill with our data to test against the test
		// data set.
		dataSet.addTable(queriedTable);
		return dataSet;
	}

	/**
	 * <p>
	 * Helper method to write out DBUnit DataSets.
	 * </p>
	 * <p>
	 * This method has been used to create the test data sets in XML format.
	 * Another purpose is to be able to have a quick look on generated data in
	 * order to check whether it is correct (e.g. before using as test data).
	 * </p>
	 * 
	 * @param dataSet
	 * @param filename
	 */
	public static void writeDataSetToXMLFile(IDataSet dataSet, String filename) {
		File outputFile = new File(filename);
		try {
			ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataSet);
			// replacementDataSet.addReplacementObject(null, "[NULL]");
			FlatXmlDataSet.write(replacementDataSet, new FileOutputStream(outputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DataSetException e) {
			e.printStackTrace();
		}
	}

	/**
	 * <p>
	 * Helper methods to load stored XML data sets from file.
	 * </p>
	 * 
	 * @param filename
	 * @return
	 * @throws DataSetException
	 * @throws FileNotFoundException
	 */
	public static IDataSet loadDataSetFromXMLFile(String filename) throws DataSetException, FileNotFoundException {
		// To deal with null values we employ a ReplacemenetDataSet. The
		// FlatXMLDataSet would just leave null values out.
		ReplacementDataSet replacementDataSet = new ReplacementDataSet(
				new FlatXmlDataSetBuilder().build(new FileInputStream(filename)));
		replacementDataSet.addReplacementObject("[NULL]", null);
		replacementDataSet.setStrictReplacement(true);
		return replacementDataSet;
	}

}
