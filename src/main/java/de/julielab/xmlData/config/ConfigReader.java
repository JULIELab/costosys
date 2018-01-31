/**
 * ConfigurationParser.java
 *
 * Copyright (c) 2011, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler/hellrich
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 22.03.2011
 **/

package de.julielab.xmlData.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximpleware.AutoPilot;
import com.ximpleware.NavException;
import com.ximpleware.VTDException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XMLModifier;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import de.julielab.xml.JulieXMLTools;

/**
 * This class reads an xml configuration file, containing the definition of a
 * database connection and the fields used in the database. It provides those
 * definitions as specialized objects.
 * 
 * @author hellrich
 */
public class ConfigReader {

	private static final Logger LOG = LoggerFactory
			.getLogger(FieldConfig.class);
	private final static int BUFFER_SIZE = 1000;
	public static final String DEFAULT_DEFINITION = "/defaultConfiguration.xml";

	public static final String XPATH_ACTIVE_TABLE_SCHEMA = "//activeTableSchema";
	public static final String XPATH_ACTIVE_DB = "//activeDBConnection";
	public static final String XPATH_ACTIVE_PG_SCHEMA = "//activePostgresSchema";
	public static final String XPATH_MAX_CONNS = "//maxActiveDBConnections";

	public static final String XPATH_CONF_DBS = "//DBConnections";
	public static final String XPATH_CONF_SCHEMAS = "//tableSchemas";

	public static final String XPATH_CONF_DB = "//DBConnection";
	public static final String XPATH_CONF_SCHEMA = "//tableSchema";

	private static final int INDEX_SCHEMA = 0;
	private static final int INDEX_DB = 1;
	private static final int INDEX_PG_SCHEMA = 2;
	private static final int INDEX_MAX_CONNS = 3;
	private static final int INDEX_DATA_TABLE = 4;
	private static final int INDEX_DATA_SCHEMA = 5;

	private static final String XPATH_DATA_TABLE = "//dataTable";
	private static final String XPATH_DATA_SCHEMA = "//activeDataPostgresSchema";
	private static final String ATTRIBUTE_NAME = "name";

	private FieldConfigurationManager fieldConfigs;
	private DBConfig dbConf;
	private String activeDataTable;
	private String activeSchemaName;
	private byte[] mergedConfigData;
	private String activeDataSchema;
	private List<String> schemaNames;

	public ConfigReader(InputStream def) {
		try {
			byte[] defaultConfData = null;
			byte[] userConfData = null;

			InputStream is = getClass().getResourceAsStream(DEFAULT_DEFINITION);
			defaultConfData = IOUtils.toByteArray(is);
			is.close();
			// check if the user gave a table schema definition;
			// if not, the default values will be used
			if (def != null) {
				userConfData = IOUtils.toByteArray(def);
				def.close();
			}
			mergedConfigData = mergeConfigData(defaultConfData, userConfData);
			
			schemaNames = getAllSchemaNames(mergedConfigData);

			// Creating
			fieldConfigs = new FieldConfigurationManager();
			for (String schemaName : schemaNames)
				fieldConfigs.put(schemaName, new FieldConfig(mergedConfigData,
						schemaName));
			dbConf = new DBConfig(mergedConfigData);
			activeDataTable = ConfigBase.getActiveConfig(mergedConfigData,
					XPATH_DATA_TABLE);
			activeDataSchema = ConfigBase.getActiveConfig(mergedConfigData,
					XPATH_DATA_SCHEMA);
			activeSchemaName = ConfigBase.getActiveConfig(mergedConfigData,
					XPATH_ACTIVE_TABLE_SCHEMA);
			
			LOG.debug("Active data table: {}", activeDataTable);
			LOG.debug("Active Postgres data schema: {}", activeDataSchema);
			LOG.debug("Active table schema: {}", activeSchemaName);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (VTDException e) {
			LOG.error("Parsing of configuration file failed:", e);
		}

	}

	/**
	 * @param mergedConfigData
	 * @return
	 */
	private List<String> getAllSchemaNames(byte[] mergedConfigData)
			throws VTDException {
		List<String> schemaNames = new ArrayList<String>();

		VTDGen vg = new VTDGen();
		vg.setDoc(mergedConfigData);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		// Navigates through schema elements
		AutoPilot schemaAP = new AutoPilot(vn);
		schemaAP.selectXPath(XPATH_CONF_SCHEMA);
		// Returns the name attribute value for current schema, navigated to by
		// schemaAP.
		AutoPilot schemaNameAP = new AutoPilot(vn);
		schemaNameAP.selectXPath("@name");

		while (schemaAP.evalXPath() != -1)
			schemaNames.add(schemaNameAP.evalXPathToString());

		return schemaNames;
	}

	/**
	 * Inserts the user schemes into the default configuration. This makes all
	 * data available in one place, which is useful for referencing default
	 * values from within a user configuration.
	 * 
	 * @param defaultConfData
	 *            - prepared default configuration file
	 * @param userConfData
	 *            - prepared user specific configuration file
	 * @param xpath
	 *            - path to the part to merge
	 * @return - the merged configuration
	 * @throws VTDException
	 * @throws IOException
	 */
	protected static byte[] mergeConfigData(byte[] defaultConfData,
			byte[] userConfData) throws VTDException, IOException {
		VTDGen vg = new VTDGen();
		vg.setDoc(defaultConfData);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		AutoPilot ap = new AutoPilot(vn);

		if (userConfData == null)
			return defaultConfData;

		XMLModifier xm = new XMLModifier(vn);

		// Get user defined schema and DB connection data.
		byte[][] userDefs = extractConfigData(userConfData);

		// Add schema data to the default configuration.
		if (userDefs[INDEX_SCHEMA] != null) {
			ap.selectXPath(XPATH_CONF_SCHEMA);
			if (ap.evalXPath() != -1) {
				xm.insertAfterElement(userDefs[INDEX_SCHEMA]);
			}
		}

		// Add DB connection data to the default configuration.
		if (userDefs[INDEX_DB] != null) {
			ap.selectXPath(XPATH_CONF_DB);
			if (ap.evalXPath() != -1) {
				xm.insertAfterElement(userDefs[INDEX_DB]);
			}
		}

		// Get active table schema, active data postgres schema, active DB
		// connection and more
		// from the user configuration, if these declarations exist.
		String[] activeConfs = getActiveConfigurations(userConfData);

		// Insert the active configurations into the merged configuration, thus
		// overwriting the defaults.
		if (activeConfs[INDEX_SCHEMA].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_ACTIVE_TABLE_SCHEMA, activeConfs[INDEX_SCHEMA]);
			if (newTextIndex == -1)
				throw new IllegalStateException(
						"Unexpected error: The default configuration does not define an active table schema. Please define an active table schema in your user configuration.");
		}
		if (activeConfs[INDEX_DB].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_ACTIVE_DB, activeConfs[INDEX_DB]);
			if (newTextIndex == -1) {
//				throw new IllegalStateException(
//						"Unexpected error: The default configuration does not define an active database connection. Please define an active DB connection in your user configuration.");
				LOG.warn("The default configuration does not define an active database connection.");
			}
			
		}
		if (activeConfs[INDEX_PG_SCHEMA].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_ACTIVE_PG_SCHEMA, activeConfs[INDEX_PG_SCHEMA]);
			if (newTextIndex == -1)
				throw new IllegalStateException(
						"Unexpected error: The default configuration does not define an active Postgres schema. Please define an active Postgres schema in your user configuration.");
		}
		if (activeConfs[INDEX_MAX_CONNS].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_MAX_CONNS, activeConfs[INDEX_MAX_CONNS]);
			if (newTextIndex == -1) {
//				throw new IllegalStateException(
//						"Unexpected error: The default configuration does not define a maximal number of database connections. Please define a maximal number of connections in your user configuration.");
				LOG.warn("Unexpected error: The default configuration does not define a maximal number of database connections");
			}
		}
		if (activeConfs[INDEX_DATA_TABLE].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_DATA_TABLE, activeConfs[INDEX_DATA_TABLE]);
			if (newTextIndex == -1)
				throw new IllegalStateException(
						"Unexpected error: The default configuration does not define a _data table. Please define a _data table in your user configuration.");
		}
		
		if (activeConfs[INDEX_DATA_SCHEMA].length() > 0) {
			int newTextIndex = JulieXMLTools.setElementText(vn, ap, xm,
					XPATH_DATA_SCHEMA, activeConfs[INDEX_DATA_SCHEMA]);
			if (newTextIndex == -1)
				throw new IllegalStateException(
						"Unexpected error: The default configuration does not define an active data Postgres schema. Please define a data postgres schema in your user configuration.");
		}

		// Test validity of merged xml (no doublets)
		vn = xm.outputAndReparse();

		String doublet = getDoublet(vn, XPATH_CONF_SCHEMA);
		if (doublet != null)
			throw new IllegalStateException(
					"Unexpected error: You may not define "
							+ doublet
							+ "as this schema is already defined in the default config!");

		doublet = getDoublet(vn, XPATH_CONF_DB);
		if (doublet != null)
			throw new IllegalStateException(
					"Unexpected error: You may not define "
							+ doublet
							+ "as this connection is already defined in the default config!");

		// Return the merged configuration data.
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		xm.output(os);
		return os.toByteArray();
	}

	private static String getDoublet(VTDNav vn, String xpath) {
		String doublet = "";
		AutoPilot ap = new AutoPilot(vn);
		try {
			ap.selectXPath(xpath);
			int index = ap.evalXPath();
			String name = null;
			Set<String> found = new HashSet<String>();
			while (index != -1) {
				int attrIndex = vn.getAttrVal(ATTRIBUTE_NAME);
				if (attrIndex != -1) {
					name = vn.toString(attrIndex);
					if (found.contains(name))
						doublet = doublet.concat(name).concat(", ");
					else
						found.add(name);
				}
				index = ap.evalXPath();
			}
		} catch (XPathParseException e) {
			e.printStackTrace();
		} catch (XPathEvalException e) {
			e.printStackTrace();
		} catch (NavException e) {
			e.printStackTrace();
		}
		return doublet.equals("") ? null : doublet;
	}

	/**
	 * Extracts the active configuration names (e.g. table schema) from the
	 * configuration data given by <code>confData</code>. Returns an array of
	 * active configuration names where the position of a specific active
	 * configuration name in the array is determined by the constants
	 * <code>INDEX_SCHEMA</code>, <code>INDEX_DB</code> etc.
	 * 
	 * @param confData
	 *            Configuration data to extract active configuration names from.
	 * @return A String array with the names of active configurations. The Array
	 *         is index by the <code>INDEX_XXX</code> constants.
	 * @throws VTDException
	 *             If something concerning the parsing and value extraction goes
	 *             wrong.
	 */
	private static String[] getActiveConfigurations(byte[] confData)
			throws VTDException {
		VTDGen vg = new VTDGen();
		vg.setDoc(confData);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		AutoPilot ap = new AutoPilot(vn);

		String[] activeConfigurations = new String[6];
		ap.selectXPath(XPATH_ACTIVE_TABLE_SCHEMA);
		activeConfigurations[INDEX_SCHEMA] = ap.evalXPathToString();

		ap.selectXPath(XPATH_ACTIVE_DB);
		activeConfigurations[INDEX_DB] = ap.evalXPathToString();

		ap.selectXPath(XPATH_ACTIVE_PG_SCHEMA);
		activeConfigurations[INDEX_PG_SCHEMA] = ap.evalXPathToString();

		ap.selectXPath(XPATH_MAX_CONNS);
		activeConfigurations[INDEX_MAX_CONNS] = ap.evalXPathToString();
		
		ap.selectXPath(XPATH_DATA_TABLE);
		activeConfigurations[INDEX_DATA_TABLE] = ap.evalXPathToString();

		ap.selectXPath(XPATH_DATA_SCHEMA);
		activeConfigurations[INDEX_DATA_SCHEMA] = ap.evalXPathToString();
		
		return activeConfigurations;
	}

	/**
	 * Retrieves XML elements (determined by the used path) from the
	 * configuration.
	 * 
	 * @param confData
	 *            - prepared XML
	 * @param xpath
	 *            - path to the retrieved configuration element
	 * @return - the retrieved element
	 * @throws IOException
	 * @throws VTDException
	 */
	protected static byte[][] extractConfigData(byte[] confData)
			throws IOException, VTDException {
		// Allocate space for schema and DB connection data.
		byte[][] configData = new byte[2][];
		VTDGen vg = new VTDGen();
		vg.setDoc(confData);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		AutoPilot ap = new AutoPilot(vn);

		// Get schema definition data.
		ap.selectXPath(XPATH_CONF_SCHEMAS);

		if (ap.evalXPath() != -1) {
			String fragment = JulieXMLTools.getFragment(vn, JulieXMLTools.CONTENT_FRAGMENT,
					true);
			configData[INDEX_SCHEMA] = fragment.getBytes();
		}

		// Get database connection data.
		ap.selectXPath(XPATH_CONF_DBS);

		if (ap.evalXPath() != -1) {
			String fragment = JulieXMLTools.getFragment(vn, JulieXMLTools.CONTENT_FRAGMENT,
					true);
			configData[INDEX_DB] = fragment.getBytes();
		}

		return configData;
	}

	/**
	 * Accessing the Database Configuration
	 * 
	 * @return - DatabaseConfig Object
	 */
	public DBConfig getDatabaseConfig() {
		return dbConf;
	}

	/**
	 * <p>
	 * Accessing the Field Definitions.
	 * </p>
	 * <p>
	 * The returned map consists of pairs in the form
	 * <code>(schemaName, fieldConfig)</code> where <code>schemaName</code> is
	 * the name of the table schema represented by <code>fieldConfig</code>.
	 * </p>
	 * 
	 * @return - A map containing all table schemas in the default and in the
	 *         user configuration.
	 */
	public FieldConfigurationManager getFieldConfigs() {
		return fieldConfigs;
	}

	/**
	 * @return the activeDataTable
	 */
	public String getActiveDataTable() {
		return activeDataTable;
	}

	public String getActiveDataSchema() {
		return activeDataSchema;
	}

	/**
	 * @return the activeSchemaName
	 */
	public String getActiveSchemaName() {
		return activeSchemaName;
	}

	/**
	 * @return the mergedConfigData
	 */
	public byte[] getMergedConfigData() {
		return mergedConfigData;
	}
	
	public List<String> getTableSchemaNames() {
		return schemaNames;
	}

}
