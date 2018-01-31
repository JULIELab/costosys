/**
 * DataBaseRetriever.java
 *
 * Copyright (c) 2010, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: hellrich
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 19.11.2010
 **/

package de.julielab.xmlData.dataBase;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import de.julielab.hiddenConfig.HiddenConfig;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import de.julielab.xmlData.Constants;
import de.julielab.xmlData.cli.TableNotFoundException;
import de.julielab.xmlData.config.ConfigReader;
import de.julielab.xmlData.config.DBConfig;
import de.julielab.xmlData.config.FieldConfig;
import de.julielab.xmlData.config.FieldConfigurationManager;

/**
 * This class creates a connection with a database and allows for convenient
 * queries and commands. <br>
 * Database layout and returned columns are specified by a configuration file.
 * The class was developed for a PostgreSQL back-end, using another database
 * server may require modifications.<br>
 * Queries use up to 3 threads for higher performance and a connection pool is
 * used for higher performance if multiple instances are deployed simultaneous.
 * 
 * Visit
 * <code>http://commons.apache.org/dbcp/apidocs/org/apache/commons/dbcp/package-
 * summary.html#package_description<\code> for more information about the
 * connection pooling.
 * 
 * 
 * @author hellrich, faessler
 */
public class DataBaseConnector {

	public static final String DEFAULT_PIPELINE_STATE = "<none>";

	/**
	 * Size of the batches used for data retrieval from the database, value is
	 * optimized for xml-clobs in postgres on 2010 hardware.
	 */
	private static final int DEFAULT_QUERY_BATCH_SIZE = 1000;

	/**
	 * Size of the byte buffer used for reading xml into vtd (xml parser)
	 */
	private final static int BUFFER_SIZE = 1000;

	private static final String DEFAULT_FIELD = "xml";
	private static final String DEFAULT_TABLE = Constants.DEFAULT_DATA_TABLE_NAME;

	// For import
	private static Logger LOG = LoggerFactory.getLogger(DataBaseConnector.class);
	private static final int commitBatchSize = 10000;
	private static Thread commitThread = null;
	public static final int PMID_IN_ARRAY = 0;
	public static final int XML_IN_ARRAY = 1;
	/**
	 * Used as a hack for the not-yet-published EMNLP-Paper. In the meantime, a more
	 * sophisticated system has been implemented (EF, 18.01.2012)
	 */
	@Deprecated
	public static final int META_IN_ARRAY = 2;
	private static final int RETRIEVE_MARK_LIMIT = 1000;
	private static final int ID_SUBLIST_SIZE = 1000;

	/**
	 * A set of field definitions read from a configuration XML file. Contains the
	 * name of each field as well as a source for the field's value.
	 */
	// private FieldConfig fieldConfig;
	/**
	 * Sometimes it is necessary to manage multiple data tables with different field
	 * schemas. fieldConfigs contains all field schema names in the configuration,
	 * mapped to the corresponding FieldConfig instance.
	 */
	private FieldConfigurationManager fieldConfigs;
	private DBConfig dbConfig;
	private String activeDataSchema;
	private String activeDataTable;
	private String activeTableSchema;
	private byte[] effectiveConfiguration;
	private int queryBatchSize = DEFAULT_QUERY_BATCH_SIZE;
	private String dbURL;
	private String user;
	private String password;

	private DataSource dataSource;

	private ConfigReader config;

	private static final LinkedHashMap<String, String> subsetColumns;

	static {
		subsetColumns = new LinkedHashMap<>();
		subsetColumns.put(Constants.LOG, "text");
		subsetColumns.put(Constants.IS_PROCESSED, "boolean DEFAULT false");
		subsetColumns.put(Constants.IN_PROCESS, "boolean DEFAULT false");
		subsetColumns.put(Constants.LAST_COMPONENT, "text DEFAULT '" + DEFAULT_PIPELINE_STATE + "'");
		subsetColumns.put(Constants.HAS_ERRORS, "boolean DEFAULT false");
		subsetColumns.put(Constants.PID, "character varying(10)");
		subsetColumns.put(Constants.HOST_NAME, "character varying(100)");
		subsetColumns.put(Constants.PROCESSING_TIMESTAMP, "timestamp without time zone");
	}

	private static final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();

	/**************************************************************************
	 *************************** Constructors ********************************
	 **************************************************************************/

	/**
	 * This class creates a connection with a database and allows for convenient
	 * queries and commands.
	 * 
	 * @param configStream
	 *            used to read the configuration for this connector instance
	 */
	public DataBaseConnector(InputStream configStream) {
		config = new ConfigReader(configStream);
		dbConfig = config.getDatabaseConfig();
		this.dbURL = dbConfig.getUrl();
		this.fieldConfigs = config.getFieldConfigs();
		this.activeDataSchema = config.getActiveDataSchema();
		this.activeDataTable = this.activeDataSchema + "." + config.getActiveDataTable();
		this.activeTableSchema = config.getActiveSchemaName();
		this.effectiveConfiguration = config.getMergedConfigData();

		HiddenConfig hc = new HiddenConfig();
		if (!StringUtils.isBlank(dbConfig.getActiveDatabase())) {
			this.user = hc.getUsername(dbConfig.getActiveDatabase());
			this.password = hc.getPassword(dbConfig.getActiveDatabase());
			LOG.info("Connecting to " + this.dbURL + " as " + this.user);
		} else {
			LOG.warn(
					"No active database configured in configuration file or configuration file is empty or does not exist.");
		}
	}

	public ConfigReader getConfig() {
		return config;
	}

	/**
	 * This class creates a connection with a database and allows for convenient
	 * queries and commands.
	 * 
	 * @param configStream
	 *            used to read the configuration for this connector instance
	 * @param queryBatchSize
	 *            background threads are utilized to speed up queries, this
	 *            parameter determines the number of pre-fetched entries
	 * 
	 */
	public DataBaseConnector(InputStream configStream, int queryBatchSize) {
		this(configStream);
		this.queryBatchSize = queryBatchSize;
	}

	/**
	 * This class creates a connection with a database and allows for convenient
	 * queries and commands.
	 * 
	 * @param dbUrl
	 *            the url of the database
	 * @param user
	 *            the username for the db
	 * @param password
	 *            the password for the username
	 * @param fieldDefinition
	 *            <code>InputStream<code> containing data of a configuration file
	 */
	public DataBaseConnector(String dbUrl, String user, String password, String pgSchema, InputStream fieldDefinition) {
		this(dbUrl, user, password, pgSchema, DEFAULT_QUERY_BATCH_SIZE, fieldDefinition);
	}

	public DataBaseConnector(String serverName, String dbName, String user, String password, String pgSchema,
			InputStream fieldDefinition) {
		this(serverName, dbName, user, password, pgSchema, DEFAULT_QUERY_BATCH_SIZE, fieldDefinition);
	}

	/**
	 * This class creates a connection with a database and allows for convenient
	 * queries and commands.
	 * 
	 * @param dbUrl
	 *            the url of the database
	 * @param user
	 *            the username for the db
	 * @param password
	 *            the password for the username
	 * @param queryBatchSize
	 *            background threads are utilized to speed up queries, this
	 *            parameter determines the number of pre-fetched entries
	 * @param driver
	 *            name of the driver to load, only postgres was tested!
	 * 
	 * @param configStream
	 *            used to read the configuration for this connector instance
	 */
	public DataBaseConnector(String dbUrl, String user, String password, String pgSchema, int queryBatchSize,
			InputStream configStream) {
		this(configStream, queryBatchSize);
		// Manually entered values have priority.
		setCredentials(dbUrl, user, password, pgSchema);
	}

	public DataBaseConnector(String serverName, String dbName, String user, String password, String pgSchema,
			int queryBatchSize, InputStream configStream) {
		this(configStream, queryBatchSize);
		// Manually entered values have priority.
		String dbUrl = null;
		if (dbName != null && serverName != null)
			dbUrl = "jdbc:postgresql://" + serverName + ":5432/" + dbName;
		else {
			if (dbName != null)
				dbUrl = dbConfig.getUrl().replaceFirst("/[^/]+$", "/" + dbName);
			if (serverName != null)
				dbUrl = dbConfig.getUrl().replaceFirst("(.*//)[^/:]+(.*)", "$1" + serverName + "$2");
		}

		setCredentials(dbUrl, user, password, pgSchema);
	}

	/**
	 * This class creates a connection with a database and allows for convenient
	 * queries and commands.
	 * 
	 * @param dbUrl
	 *            the url of the database
	 * @param user
	 *            the username for the db
	 * @param password
	 *            the password for the username
	 */
	public DataBaseConnector(String dbUrl, String user, String password) {
		this(dbUrl, user, password, null, DEFAULT_QUERY_BATCH_SIZE, null);
	}

	/**************************************************************************
	 ************************* Helper Methods ********************************
	 **************************************************************************/

	/**
	 * @param dbUrl
	 * @param user
	 * @param password
	 * @param pgSchema
	 */
	private void setCredentials(String dbUrl, String user, String password, String pgSchema) {
		if (dbUrl != null)
			this.dbURL = dbUrl;
		if (user != null)
			this.user = user;
		if (password != null)
			this.password = password;
		if (pgSchema != null)
			setActivePGSchema(pgSchema);
		if ((dbUrl != null) || (user != null) || (password != null) || (pgSchema != null))
			LOG.info("Connecting to " + this.dbURL + " as " + this.user + " in Postgres Schema " + pgSchema);
	}

	public void setDbURL(String uri) {
		dbURL = uri;
	}

	public void setHost(String host) {
		if (host != null) {
			dbURL = dbURL.replaceFirst("(.*//)[^/:]+(.*)", "$1" + host + "$2");
			LOG.debug("Setting database host to {}. DB URL is now {}", host, dbURL);
		}
	}

	public void setPort(String port) {
		setPort(Integer.parseInt(port));
	}

	public void setPort(Integer port) {
		if (port != null) {
			this.dbURL = dbURL.replaceFirst(":[0-9]+", ":" + port);
			LOG.debug("Setting database port to {}. DB URL is now {}", port, dbURL);
		}
	}

	public void setUser(String user) {
		this.user = user;
		LOG.debug("Setting database user for {} to {}", this.dbURL, user);
	}

	public void setPassword(String password) {
		this.password = password;
		LOG.debug("Changing database password.");
	}

	/**
	 * @return A Connection to the database.
	 */
	public Connection getConn() {

		Connection conn = null;
		if (null == dataSource) {
			LOG.debug("Setting up connection pool data source");
			HikariDataSource ds = pools.getOrDefault(dbURL, new HikariDataSource());
			if (ds.isClosed())
				ds = new HikariDataSource();
			pools.put(dbURL, ds);
			ds.setJdbcUrl(dbURL);
			ds.setUsername(user);
			ds.setPassword(password);
			ds.setConnectionTestQuery("SELECT TRUE");
			ds.setMaximumPoolSize(dbConfig.getMaxConnections());
			dataSource = ds;
		}

		try {
			LOG.trace("Waiting for SQL connection to become free...");
			conn = dataSource.getConnection();
			// conn = DriverManager.getConnection(fullURI);
			LOG.trace("SQL connection obtained.");
			Statement stm = conn.createStatement();
			if (!schemaExists(dbConfig.getActivePGSchema(), conn))
				createSchema(dbConfig.getActivePGSchema(), conn);
			if (!schemaExists(dbConfig.getActiveDataPGSchema(), conn))
				createSchema(dbConfig.getActiveDataPGSchema(), conn);
			stm.execute(String.format("SET search_path TO %s", dbConfig.getActivePGSchema()));
			stm.close();

		} catch (SQLException e) {
			LOG.error("Could not connect with " + dbURL);
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * @return the activeDataTable
	 */
	public String getActiveDataTable() {
		return activeDataTable;
	}

	/**
	 * <p>
	 * Returns the effective XML configuration as a <code>byte[]</code>.
	 * </p>
	 * <p>
	 * The effective configuration consists of the default configuration and the
	 * given user configuration as well (merged by the ConfigReader in the
	 * constructor).
	 * </p>
	 * 
	 * @return the effectiveConfiguration
	 */
	public byte[] getEffectiveConfiguration() {
		return effectiveConfiguration;
	}

	public String getActiveDataPGSchema() {
		return activeDataSchema;
	}

	public String getActivePGSchema() {
		return dbConfig.getActivePGSchema();
	}

	public String getActiveTableSchema() {
		return activeTableSchema;
	}

	/**************************************************************************
	 *************************** for DBReader ********************************
	 **************************************************************************/

	/**
	 * <p>
	 * Retrieves from a subset-table <code>limit</code> primary keys whose rows are
	 * not marked to be in process or finished being processed and sets the rows of
	 * the retrieved primary keys as being "in process".
	 * </p>
	 * <p>
	 * The table is locked during this transaction. Locking and marking ensure that
	 * every primary key will be returned exactly once. Remember to remove the marks
	 * if you want to use the subset again ;)
	 * </p>
	 * 
	 * @param subsetTableName
	 *            - name of a table, conforming to the subset standard
	 * @param hostName
	 *            - will be saved in the subset table
	 * @param pid
	 *            - will be saved in the subset table
	 * 
	 * @see #retrieveAndMark(String, String, String, int, boolean)
	 * 
	 * @return An ArrayList of pmids which have not yet been processed
	 * 
	 */
	public List<Object[]> retrieveAndMark(String subsetTableName, String readerComponent, String hostName, String pid) {
		return retrieveAndMark(subsetTableName, readerComponent, hostName, pid, RETRIEVE_MARK_LIMIT, null);
	}

	/**
	 * <p>
	 * Retrieves primary keys from a subset table and marks them as being "in
	 * process". The table schema - and thus the form of the primary keys - is
	 * assumed to match the active table schema determined in the configuration
	 * file.
	 * </p>
	 * The table is locked during this transaction. Locking and marking ensure that
	 * every primary key will be returned exactly once. Remember to remove the marks
	 * if you want to use the subset again ;)
	 * 
	 * @param subsetTableName
	 *            - name of a table, conforming to the subset standard
	 * @param hostName
	 *            - will be saved in the subset table
	 * @param pid
	 *            - will be saved in the subset table
	 * 
	 * @param limit
	 *            - batchsize for marking/retrieving
	 * @param order
	 *            - determines an ordering. Default order (which may change over
	 *            time) when this parameter is null or empty.
	 * 
	 * @return An ArrayList of primary keys which have not yet been processed.
	 * 
	 * @see #retrieveAndMark(String, String, String, String, int, String)
	 */
	public List<Object[]> retrieveAndMark(String subsetTableName, String readerComponent, String hostName, String pid,
			int limit, String order) {
		return retrieveAndMark(subsetTableName, activeTableSchema, readerComponent, hostName, pid, limit, order);
	}

	/**
	 * <p>
	 * Retrieves from a subset-table <code>limit</code> primary keys whose rows are
	 * not marked to be in process or finished being processed and sets the rows of
	 * the retrieved primary keys as being "in process".
	 * </p>
	 * <p>
	 * The following parameters may be set:
	 * <ul>
	 * <li><code>limit</code> - sets the maximum number of primary keys retrieved
	 * <li><code>order</code> - determines whether to retrieve the primary keys in a
	 * particular order. Note that the default order of rows is undefined. If you
	 * need the same order in every run, you should specify some ordering as an SQL
	 * 'ORDER BY' statement. When <code>order</code> is not prefixed with 'ORDER BY'
	 * (case ignored), it will be inserted.
	 * </ul>
	 * </p>
	 * <p>
	 * The table is locked during this transaction. Locking and marking ensure that
	 * every primary key will be returned exactly once. Remember to remove the marks
	 * if you want to use the subset again ;)
	 * </p>
	 * 
	 * @param subsetTableName
	 *            - name of a table, conforming to the subset standard
	 * @param hostName
	 *            - will be saved in the subset table
	 * @param pid
	 *            - will be saved in the subset table
	 * 
	 * @param limit
	 *            - batchsize for marking/retrieving
	 * @param order
	 *            - determines an ordering. Default order (which may change over
	 *            time) when this parameter is null or empty.
	 * 
	 * @return An ArrayList of primary keys which have not yet been processed.
	 * 
	 */
	public List<Object[]> retrieveAndMark(String subsetTableName, String schemaName, String readerComponent,
			String hostName, String pid, int limit, String order) {
		List<Object[]> ids = new ArrayList<Object[]>(limit);
		String sql = null;
		Connection conn = null;
		boolean idsRetrieved = false;
		while (!idsRetrieved) {
			try {
				FieldConfig fieldConfig = fieldConfigs.get(schemaName);
				conn = getConn();

				conn.setAutoCommit(false);
				Statement st = conn.createStatement();
				String orderCommand = order == null ? "" : order;
				if (!orderCommand.equals("") && !orderCommand.trim().toUpperCase().startsWith("ORDER BY"))
					orderCommand = "ORDER BY " + orderCommand;
				String joinStatement = Stream.of(fieldConfig.getPrimaryKey()).map(pk -> {
					return "t." + pk + "=subquery." + pk;
				}).collect(Collectors.joining(" AND "));
				String returnColumns = Stream.of(fieldConfig.getPrimaryKey()).map(pk -> {
					return "t." + pk;
				}).collect(Collectors.joining(","));

				// following
				// http://dba.stackexchange.com/questions/69471/postgres-update-limit-1
				sql = "UPDATE " + subsetTableName + " AS t SET " + Constants.IN_PROCESS + " = TRUE, "
						+ Constants.LAST_COMPONENT + " = '" + readerComponent + "', " + Constants.HOST_NAME + " = \'"
						+ hostName + "\', " + Constants.PID + " = \'" + pid + "\'," + Constants.PROCESSING_TIMESTAMP
						+ " = 'now' FROM (SELECT " + fieldConfig.getPrimaryKeyString() + " FROM " + subsetTableName
						+ " WHERE " + Constants.IN_PROCESS + " = FALSE AND "
						// eigentlich wollen wir anstelle von FOR UPDATE sogar:
						// FOR UPDATE SKIP LOCKED in PostgreSQL 9.5 <---!!
						+ Constants.IS_PROCESSED + " = FALSE " + orderCommand + " LIMIT " + limit
						+ " FOR UPDATE) AS subquery WHERE " + joinStatement + " RETURNING " + returnColumns;
				try (ResultSet res = st.executeQuery(sql)) {
					String[] pks = fieldConfig.getPrimaryKey();
					while (res.next()) {
						Object[] values = new String[pks.length];
						for (int i = 0; i < pks.length; i++) {
							values[i] = res.getObject(i + 1);
						}
						ids.add(values);
					}
					idsRetrieved = true;
				}
				conn.commit();
			} catch (SQLException e) {
				// It is possible to run into deadlocks with the above query. Then, one process
				// will be canceled and we get an exception. If so, just log is and try again.
				if (!e.getMessage().contains("deadlock detected") && (e.getNextException() == null
						|| !e.getNextException().getMessage().contains("deadlock detected"))) {
					LOG.error(
							"Error while retrieving document IDs and marking them to be in process. Sent SQL command: {}.",
							sql, e);
					SQLException nextException = e.getNextException();
					if (null != nextException)
						LOG.error("Next exception: {}", nextException);
					// this is not the deadlock error; break the loop
					break;
				} else {
					LOG.debug(
							"Database deadlock has been detected while trying to retrieve document IDs and marking them to be processed. Tying again.");
					// We need to close the current, failed, transaction and start a new one for the
					// new try.
					try {
						conn.commit();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("The following IDs were retrieved from table {}: {}", subsetTableName, ids.stream().map(Arrays::toString).collect(Collectors.joining("; ")));
		}
		return ids;
	}

	/**
	 * 
	 * @param subsetTableName
	 * @return
	 * @see #countUnprocessed(String)
	 */
	public int countUnprocessed(String subsetTableName) {
		return countUnprocessed(subsetTableName, activeTableSchema);
	}

	/**
	 * Counts the unprocessed rows in a subset table
	 * 
	 * @param subsetTableName
	 *            - name of the subset table
	 * @return - number of rows
	 */
	public int countUnprocessed(String subsetTableName, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		int rows = 0;
		Connection conn = getConn();
		try {
			ResultSet res = conn.createStatement().executeQuery(
					// as we are just looking for any unprocessed documents it
					// is
					// sufficient - even in the case of multiple primary key
					// elements - to use the name of the first element
					// in this command
					"SELECT count(" + fieldConfig.getPrimaryKey()[0] + ")" + " FROM " + subsetTableName + " WHERE "
							+ Constants.PROCESSED + " = FALSE;");
			if (res.next())
				rows = res.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return rows;
	}

	public int countRowsOfDataTable(String tableName, String whereCondition) {
		return countRowsOfDataTable(tableName, whereCondition, activeTableSchema);
	}

	public int countRowsOfDataTable(String tableName, String whereCondition, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		int rows = 0;
		Connection conn = getConn();
		try {
			if (whereCondition != null) {
				whereCondition = whereCondition.trim();
				if (!whereCondition.toUpperCase().startsWith("WHERE"))
					whereCondition = " WHERE " + whereCondition;
				else
					whereCondition = " " + whereCondition;
			} else
				whereCondition = "";

			ResultSet res = conn.createStatement().executeQuery(
					"SELECT count(" + fieldConfig.getPrimaryKeyString() + ")" + " FROM " + tableName + whereCondition);
			if (res.next())
				rows = res.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return rows;
	}

	public boolean hasUnfetchedRows(String tableName) {
		return hasUnfetchedRows(tableName, activeTableSchema);
	}

	public boolean hasUnfetchedRows(String tableName, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		Connection conn = getConn();
		try {
			ResultSet res = conn.createStatement()
					.executeQuery("SELECT " + fieldConfig.getPrimaryKeyString() + " FROM " + tableName + " WHERE "
							+ Constants.IN_PROCESS + " = FALSE AND " + Constants.IS_PROCESSED + " = FALSE LIMIT 1");
			return res.next();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	/**************************************************************************
	 ******************************** Utility **********************************
	 ***************************************************************************/

	/**
	 * Deletes entries from a table
	 * 
	 * @param table
	 *            name of the table
	 * @param ids
	 *            primary key arrays defining the entries to delete
	 * @see #deleteFromTableSimplePK(String, List)
	 */
	public void deleteFromTable(String table, List<Object[]> ids) {
		String sql = "DELETE FROM " + table + " WHERE ";
		modifyTable(sql, ids);
	}

	/**
	 * Deletes entries from a table where the primary key of this table must consist
	 * of exactly one column. For deletion from tables which contain a
	 * multi-column-primary-key see {@link #deleteFromTable(String, List)}.
	 * 
	 * @param table
	 *            name of the table
	 * @param ids
	 *            primary key arrays defining the entries to delete
	 * @see #deleteFromTable(String, List)
	 */
	public <T> void deleteFromTableSimplePK(String table, List<T> ids) {
		String sql = "DELETE FROM " + table + " WHERE ";

		// Convert the given list to a list of object arrays, so it fits to
		// 'modifyTable'.
		List<Object[]> objectIds = new ArrayList<Object[]>(ids.size());
		for (T id : ids)
			objectIds.add(new Object[] { id });
		modifyTable(sql, objectIds);
	}

	/**
	 * Modifies a subset table, marking entries as processed.
	 * 
	 * @param table
	 *            name of the subset table
	 * @param ids
	 *            primary key arrays defining the entries to delete
	 */
	public void markAsProcessed(String table, List<Object[]> ids) {
		String sql = "UPDATE " + table + " SET " + Constants.PROCESSED + " = TRUE WHERE ";
		modifyTable(sql, ids);
	}

	/**
	 * <p>
	 * Executes a given SQL command (must end with "WHERE "!) an extends the
	 * WHERE-clause with the primary keys, set to the values in ids.
	 * </p>
	 * <p>
	 * Assumes that the form of the primary keys matches the definition given in the
	 * active table schema in the configuration.
	 * </p>
	 * 
	 * @param sql
	 *            a valid SQL command, ending with "WHERE "
	 * @param ids
	 *            list of primary key arrays
	 * 
	 * @see #modifyTable(String, List)
	 */
	public void modifyTable(String sql, List<Object[]> ids) {
		modifyTable(sql, ids, activeTableSchema);
	}

	/**
	 * <p>
	 * Executes a given SQL command (must end with "WHERE "!) an extends the
	 * WHERE-clause with the primary keys, set to the values in ids.
	 * </p>
	 * 
	 * @param sql
	 *            a valid SQL command, ending with "WHERE "
	 * @param ids
	 *            list of primary key arrays
	 * @param schemaName
	 *            name of the schema which defines the primary keys
	 */
	public void modifyTable(String sql, List<Object[]> ids, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		Connection conn = getConn();
		String where = StringUtils.join(fieldConfig.expandPKNames("%s = ?"), " AND ");
		String fullSQL = sql + where;
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(fullSQL);
		} catch (SQLException e) {
			LOG.error("Couldn't prepare: " + fullSQL);
			e.printStackTrace();
		}
		String[] pks = fieldConfig.getPrimaryKey();
		for (Object[] id : ids) {
			for (int i = 0; i < id.length; ++i) {
				try {
					setPreparedStatementParameterWithType(i + 1, ps, id[i], pks[i], fieldConfig);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				ps.addBatch();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			ps.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * just calls ps.setObject(position, value);
	 * 
	 * @param position
	 * @param ps
	 * @param value
	 * @param fieldName
	 * @param fieldConfig
	 * @throws SQLException
	 */
	private void setPreparedStatementParameterWithType(int position, PreparedStatement ps, Object value,
			String fieldName, FieldConfig fieldConfig) throws SQLException {
		ps.setObject(position, value);
	}

	/**
	 * Returns the name of a table referenced by an SQL-foreign-key.
	 * 
	 * @param referencingTable
	 *            the name of the table for which the foreign keys shall be checked
	 * @return the name of the first referenced table or <code>null</code> if there
	 *         is no referenced table (i.e. the passed table name denotes a data
	 *         table).
	 * @throws IllegalArgumentException
	 *             When <code>referencingTable</code> is <code>null</code>.
	 */
	public String getReferencedTable(String referencingTable) {
		if (referencingTable == null)
			throw new IllegalArgumentException("Name of referencing table may not be null.");

		String referencedTable = null;
		Connection conn = getConn();
		try {
			String pgSchema = dbConfig.getActivePGSchema();
			String tableName = referencingTable;
			if (referencingTable.contains(".")) {
				pgSchema = referencingTable.replaceFirst("\\..*$", "");
				tableName = referencingTable.substring(referencingTable.indexOf('.') + 1);
			}
			// Lowercasing of the table name since case matters but postgres
			// does lowercase on table creation.
			ResultSet imported = conn.getMetaData().getImportedKeys("", pgSchema, tableName.toLowerCase());
			// ReferencingTable.replaceFirst(".*?\\.", "") truncates a potential
			// existing schema qualification. If we don't do this and the table
			// name is qualified nothing will be found (dots in table names are
			// allowed, thus there is no automatic normalization).
			// TODO but the above assumes that the correct schema is included in
			// the dbcConfiguration and thus we can forget about the schema
			// qualification of the table name. But what if the qualification
			// has been given with intention? If nothing is found without the
			// qualification, it should be tried again with the qualification.
			if (imported.next()) {
				String pkTableSchema = imported.getString(2);
				String pkTableName = imported.getString(3);
				referencedTable = pkTableSchema != null ? pkTableSchema + "." + pkTableName : pkTableName;
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return referencedTable;
	}

	/**
	 * Creates a PostgreSQL schema
	 * <p>
	 * This private method is called by the SQL <code>Connection</code> source, thus
	 * it takes the <code>Connection</code> as a parameter instead of getting a
	 * <code>Connection</code> on its own.
	 * </p>
	 * 
	 * @param schemaName
	 *            The name of the PostgreSQL schema to create.
	 * @param conn
	 *            Connection to the database which should be checked for the
	 *            existence of the schema <code>schemaName</code>.
	 */
	private void createSchema(String schemaName, Connection conn) {
		String sqlStr = "CREATE SCHEMA " + schemaName;
		try {
			conn.createStatement().execute(sqlStr);
			LOG.info("PostgreSQL schema \"{}\" does not exist, it is being created.", schemaName);
		} catch (SQLException e) {
			LOG.error(sqlStr);
			e.printStackTrace();
		}
	}

	/**
	 * Creates the PostgreSQL schema <code>schemaName</code> in the active database.
	 * 
	 * @param schemaName
	 *            The name of the PostgreSQL schema to create.
	 */
	public void createSchema(String schemaName) {
		Connection conn = getConn();
		createSchema(schemaName, conn);
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates a new table according to the field schema definition corresponding to
	 * the active schema name determined in the configuration.
	 * 
	 * @param tableName
	 *            the name of the new table
	 * @throws SQLException
	 */
	public void createTable(String tableName, String comment) throws SQLException {
		createTable(tableName, activeTableSchema, comment);
	}

	/**
	 * Creates a new table according to the field schema definition corresponding to
	 * the name <code>schemaName</code> given in the configuration file.
	 * 
	 * @param tableName
	 *            the name of the new table
	 * @throws SQLException
	 */
	public void createTable(String tableName, String schemaName, String comment) throws SQLException {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		ArrayList<String> columns = getTableCreationColumns(tableName, fieldConfig);

		createTable(tableName, columns, comment);

		// additionally, restrict the primary key to be unique
		// (I don't know why this is necessary, but it is required
		// for a referencing table which references several columns,
		// that these columns own a UNIQUE constraint.)
		if (fieldConfig.getPrimaryKey().length > 0)
			alterTable(String.format("ADD CONSTRAINT %s_unique UNIQUE (%s)", tableName.replace(".", ""),
					fieldConfig.getPrimaryKeyString()), tableName);
	}

	/**
	 * <p>
	 * Creates a new table according to the field schema definition corresponding to
	 * the name <code>schemaName</code> and with foreign key references to the
	 * primary key of <tt>referenceTableName</tt>.
	 * </p>
	 * <p>
	 * The primary key of the tables <tt>tableName</tt> and
	 * <tt>referenceTableName</tt> must be equal. The foreign key constraint is
	 * configured for <tt>ON DELETE CASCADE</tt> which means, when in the referenced
	 * table rows are deleted, there are also deleted in the table created by this
	 * method call.
	 * </p>
	 * 
	 * @param tableName
	 *            The name of the new table.
	 * @param referenceTableName
	 *            The table to be referenced by this table.
	 * @param schemaName
	 *            The table schema determining the structure (especially the primary
	 *            key) of the new table.
	 * @param comment
	 *            A comment for the new table.
	 * @throws SQLException
	 */
	public void createTable(String tableName, String referenceTableName, String schemaName, String comment)
			throws SQLException {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		ArrayList<String> columns = getTableCreationColumns(tableName, fieldConfig);
		columns.add(String.format("CONSTRAINT %s_fkey FOREIGN KEY (%s) REFERENCES %s ON DELETE CASCADE",
				tableName.replace(".", ""), fieldConfig.getPrimaryKeyString(), referenceTableName));

		createTable(tableName, columns, comment);

		// additionally, restrict the primary key to be unique
		// (I don't know why this is necessary, but it is required
		// for a referencing table which references several columns,
		// that these columns own a UNIQUE constraint.)
		if (fieldConfig.getPrimaryKey().length > 0)
			alterTable(String.format("ADD CONSTRAINT %s_unique UNIQUE (%s)", tableName.replace(".", ""),
					fieldConfig.getPrimaryKeyString()), tableName);
	}

	/**
	 * Creates the columns to create a table according to the table schema given by
	 * <tt>fieldConfig</tt> for use with {@link #createTable(String, List, String)}.
	 * 
	 * @param tableName
	 * @param fieldConfig
	 * @return
	 */
	private ArrayList<String> getTableCreationColumns(String tableName, FieldConfig fieldConfig) {
		ArrayList<String> columns = new ArrayList<String>();
		for (Map<String, String> field : fieldConfig.getFields()) {
			StringBuilder columnStrBuilder = new StringBuilder();
			columnStrBuilder.append(field.get(JulieXMLConstants.NAME));
			columnStrBuilder.append(" ");
			columnStrBuilder.append(field.get(JulieXMLConstants.TYPE));
			columns.add(columnStrBuilder.toString());
		}
		if (fieldConfig.getPrimaryKey().length > 0)
			columns.add(String.format("CONSTRAINT %s_pkey PRIMARY KEY (%s)", tableName.replace(".", ""),
					fieldConfig.getPrimaryKeyString()));
		return columns;
	}

	/**
	 * Creates a new table with custom columns.
	 * 
	 * @param tableName
	 *            the name of the new table
	 * @param columns
	 *            a list of Strings, each containing name, type and constraint of a
	 *            column, e.g. "foo integer primary key" as required for a valid sql
	 *            command.
	 * @throws SQLException
	 */
	private void createTable(String tableName, List<String> columns, String comment) throws SQLException {
		Connection conn = getConn();
		StringBuilder sb = new StringBuilder("CREATE TABLE " + tableName + " (");
		for (String column : columns)
			sb.append(", " + column);
		sb.append(");");
		String sqlString = sb.toString().replaceFirst(", ", "");
		try {
			Statement st = conn.createStatement();
			st.execute(sqlString);
			st.execute("COMMENT ON TABLE " + tableName + " IS \'" + comment + "\';");
		} catch (SQLException e) {
			System.err.println(sqlString);
			e.printStackTrace();
			throw e;
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * <p>
	 * Does the same as {@link #createSubsetTable(String, String, String, String)}
	 * with the exception that the assumed table schema is that of the active schema
	 * defined in the configuration file.
	 * </p>
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of the referenced table
	 * @param maxNumberRefHops
	 *            the maximum number of times a foreign key reference to a data
	 *            table may be followed
	 * @param comment
	 *            will be added to the table in the database, used to make tables
	 *            reproducable
	 * @throws SQLException
	 * @see #createSubsetTable(String, String, String, String)
	 */
	public void createSubsetTable(String subsetTable, String supersetTable, Integer maxNumberRefHops, String comment)
			throws SQLException {
		createSubsetTable(subsetTable, supersetTable, maxNumberRefHops, comment, activeTableSchema);
	}

	/**
	 * <p>
	 * Does the same as {@link #createSubsetTable(String, String, String, String)}
	 * with the exception that the assumed table schema is that of the active schema
	 * defined in the configuration file.
	 * </p>
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of the referenced table
	 * @param comment
	 *            will be added to the table in the database, used to make tables
	 *            reproducable
	 * @throws SQLException
	 * @see #createSubsetTable(String, String, String, String)
	 */
	public void createSubsetTable(String subsetTable, String supersetTable, String comment) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, comment, activeTableSchema);
	}

	/**
	 * <p>
	 * Creates an empty table referencing the primary key of the data table given by
	 * <code>superSetTable</code> or, if this is a subset table itself, the data
	 * table referenced by that table.
	 * </p>
	 * <p>
	 * To fill the empty subset table with data, use one of the
	 * <code>init[...]</code> methods offered by this class.
	 * </p>
	 * <p>
	 * Subset tables have a particular table scheme. They define a foreign key to
	 * the primary key of the referenced data table. There are the following
	 * additional columns:
	 * <table>
	 * <tr>
	 * <th>Name</th>
	 * <th>Type</th>
	 * </tr>
	 * <tr>
	 * <td>is_in_process</td>
	 * <td>boolean</td>
	 * </tr>
	 * <tr>
	 * <td>is_processed</td>
	 * <td>boolean</td>
	 * </tr>
	 * <tr>
	 * <td>last_component</td>
	 * <td>text</td>
	 * </tr>
	 * <tr>
	 * <td>log</td>
	 * <td>text</td>
	 * </tr>
	 * <tr>
	 * <td>has errors</td>
	 * <td>boolean</td>
	 * </tr>
	 * <tr>
	 * <td>pid</td>
	 * <td>character varying(10)</td>
	 * </tr>
	 * <tr>
	 * <td>host_name</td>
	 * <td>character varying(100)</td>
	 * </tr>
	 * <tr>
	 * <td>processing_timestamp</td>
	 * <td>timestamp without time zone</td>
	 * </tr>
	 * </table>
	 * </p>
	 * <p>
	 * The subset table can be used for processing, e.g. by UIMA CollectionReaders,
	 * which store information about the processing in it.
	 * <p>
	 * The actual data is located in the referenced table.
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of the referenced table
	 * @param posOfDataTable
	 *            the position of the datatable that should be referenced; the 1st
	 *            would be nearest data table, i.e. perhaps <tt>supersetTable</tt>
	 *            itself. The 2nd would be the datatable referenced by the first
	 *            data table on the reference path.
	 * @param schemaName
	 *            name of the table schema to work with (determined in the
	 *            configuration file)
	 * @param comment
	 *            will be added to the table in the database, used to make tables
	 *            reproducable
	 * @throws SQLException
	 * 
	 */
	public void createSubsetTable(String subsetTable, String supersetTable, Integer posOfDataTable, String comment,
			String schemaName) throws SQLException {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		String effectiveDataTable = getReferencedTable(supersetTable, posOfDataTable);

		ArrayList<String> columns = new ArrayList<String>();
		List<Map<String, String>> fields = fieldConfig.getFields();
		HashSet<String> pks = new HashSet<String>(Arrays.asList(fieldConfig.getPrimaryKey()));
		for (Map<String, String> field : fields) {
			String name = field.get(JulieXMLConstants.NAME);
			if (pks.contains(name))
				columns.add(name + " " + field.get(JulieXMLConstants.TYPE));
		}

		// Add the columns to the table.
		for (Entry<String, String> columnDefinition : subsetColumns.entrySet()) {
			columns.add(columnDefinition.getKey() + " " + columnDefinition.getValue());
		}
		// Define the primary key of the table.
		String pkStr = fieldConfig.getPrimaryKeyString();
		columns.add(String.format("CONSTRAINT %s_pkey PRIMARY KEY (%s)", subsetTable.replace(".", ""), pkStr));
		columns.add(String.format("CONSTRAINT %s_fkey FOREIGN KEY (%s) REFERENCES %s ON DELETE CASCADE",
				subsetTable.replace(".", ""), pkStr, effectiveDataTable));
		createTable(subsetTable, columns, comment);
		createIndex(subsetTable, Constants.IS_PROCESSED, Constants.IN_PROCESS);
	}

	/**
	 * Creates an index for table <tt>table</tt> on the given <tt>columns</tt>. The
	 * name of the index will be <tt>&lt;table&gt;_idx</tt>. It is currently not
	 * possible to create a second index since the names would collide. This would
	 * require an extension of this method for different names.
	 * 
	 * @param table
	 *            The table for which an index should be created.
	 * @param columns
	 *            The columns the index should cover.
	 * @throws SQLException
	 *             In case something goes wrong.
	 */
	public void createIndex(String table, String... columns) throws SQLException {
		Connection conn = getConn();
		String sql = String.format("CREATE INDEX %s_idx ON %s (%s)", table.replace(".", ""), table,
				String.join(",", columns));
		conn.createStatement().execute(sql);
		conn.close();
	}

	/**
	 * Gets the - possibly indirectly - referenced table of <tt>startTable</tt>
	 * where <tt>posOfDataTable</tt> specifies the position of the desired table in
	 * the reference chain starting at <tt>startTable</tt>.
	 * 
	 * @param startTable
	 * @param posOfDataTable
	 * @return
	 * @throws SQLException
	 */
	public String getReferencedTable(String startTable, Integer posOfDataTable) throws SQLException {
		if (posOfDataTable == null)
			posOfDataTable = 1;
		int currentDatatablePosition = isDataTable(startTable) ? 1 : 0;
		Set<String> blacklist = new HashSet<>();
		String effectiveDataTable = startTable;
		String lasttable = "";
		while (isSubsetTable(effectiveDataTable) || currentDatatablePosition < posOfDataTable) {
			if (blacklist.contains(effectiveDataTable)) {
				if (effectiveDataTable.equals(lasttable))
					throw new IllegalStateException(
							"The table \"" + lasttable + "\" has a foreign key on itself. This is not allowed.");
				throw new IllegalStateException(
						"Fatal error: There is a circel in the foreign key chain. The table \"" + effectiveDataTable
								+ "\" has been found twice when following the foreign key chain of the table \""
								+ startTable + "\".");
			}
			blacklist.add(effectiveDataTable);
			lasttable = effectiveDataTable;
			effectiveDataTable = getNextDataTable(effectiveDataTable);
			currentDatatablePosition++;
		}
		return effectiveDataTable;
	}

	public String getNextDataTable(String referencingTable) throws SQLException {
		String referencedTable = getReferencedTable(referencingTable);
		while (isSubsetTable(referencedTable)) {
			referencedTable = getReferencedTable(referencedTable);
		}
		return referencedTable;
	}

	public boolean isSubsetTable(String table) throws SQLException {
		try (Connection conn = getConn()) {
			String pgSchema = dbConfig.getActivePGSchema();
			String tableName = table;
			if (table.contains(".")) {
				pgSchema = table.replaceFirst("\\..*$", "");
				tableName = table.substring(table.indexOf('.') + 1);
			}
			// Do lowercase on the table name: Case matters and postgres always
			// lowercases the names on creation...
			ResultSet columns = conn.getMetaData().getColumns(null, pgSchema, tableName.toLowerCase(), null);
			int numSubsetColumnsFound = 0;
			while (columns.next()) {
				String columnName = columns.getString(4);
				if (subsetColumns.keySet().contains(columnName))
					numSubsetColumnsFound++;
			}
			return numSubsetColumnsFound == subsetColumns.size();
		}
	}

	public boolean isDataTable(String table) throws SQLException {
		return !isSubsetTable(table);
	}

	public boolean dropTable(String table) throws SQLException {
		try (Connection conn = getConn()) {
			Statement stmt = conn.createStatement();
			String sql = "DROP TABLE " + table;
			return stmt.execute(sql);
		}
	}

	/**
	 * Tests if a table exists.
	 * 
	 * @param tableName
	 *            name of the table to test
	 * @return true if the table exists, false otherwise
	 */
	public boolean tableExists(Connection conn, String tableName) {
		try {
			Statement stmt = conn.createStatement();
			String pureTableName = tableName;
			String schemaName = dbConfig.getActivePGSchema();
			if (tableName.contains(".")) {
				String[] split = tableName.split("\\.");
				schemaName = split[0];
				pureTableName = split[1];
			}
			// Lowercase the names because in Postgres they are lowercased
			// automatically when the tables are created. Thus, when not
			// lowercasing we risk to miss the correct entry.
			String sql = String.format(
					"select schemaname,tablename from pg_tables where schemaname = '%s' and tablename = '%s'",
					schemaName.toLowerCase(), pureTableName.toLowerCase());
			LOG.trace("Checking whether table {} in schema {} exists.", pureTableName, schemaName);
			LOG.trace("Sent query (names have been lowercased to match Postgres table names): {}", sql);
			ResultSet res = stmt.executeQuery(sql);
			return res.next();
		} catch (SQLException e) {
			e.printStackTrace();
			SQLException ne = e.getNextException();
			if (null != ne)
				ne.printStackTrace();
		}
		return false;

		// After that, the connection cannot be used any more because
		// "current transaction is aborted, commands ignored until end of
		// transaction block"
		// try {
		// conn.createStatement().executeQuery(
		// "SELECT * FROM " + tableName + " LIMIT 1"); // Provoking
		// return true;
		// } catch (SQLException e) {
		// return false;
		// }
	}

	/**
	 * Tests if a table exists.
	 * 
	 * @param tableName
	 *            name of the table to test
	 * @return true if the table exists, false otherwise
	 */
	public boolean tableExists(String tableName) {
		Connection conn = getConn();
		try {
			return tableExists(conn, tableName);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Tests if a schema exists.
	 * <p>
	 * This private method is called by the SQL <code>Connection</code> source, thus
	 * it takes the <code>Connection</code> as a parameter instead of getting a
	 * <code>Connection</code> on its own.
	 * </p>
	 * 
	 * @param schemaName
	 *            name of the schema to test
	 * @param conn
	 *            Connection to the database which should be checked for the
	 *            existence of the schema <code>schemaName</code>.
	 * @return true if the schema exists, false otherwise
	 */
	private boolean schemaExists(String schemaName, Connection conn) {
		try {
			ResultSet rs = conn.createStatement()
					.executeQuery("SELECT * FROM pg_namespace WHERE nspname = '" + schemaName + "'");
			return rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Tests if a schema exists.
	 * 
	 * @param schemaName
	 *            name of the schema to test
	 * @return true if the schema exists, false otherwise
	 */
	public boolean schemaExists(String schemaName) {
		Connection conn = getConn();
		boolean exists = schemaExists(schemaName, conn);
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return exists;
	}

	/**
	 * Tests if a table contains entries.
	 * 
	 * @param tableName
	 *            name of the schema to test
	 * @return true if the table has entries, false otherwise
	 */
	public boolean isEmpty(String tableName) {
		Connection conn = getConn();
		String sqlStr = "SELECT * FROM " + tableName + " LIMIT 1";
		try {
			Statement st = conn.createStatement();
			ResultSet res = st.executeQuery(sqlStr);

			return !res.next();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				LOG.error(sqlStr);
			}
		}
		return false;
	}

	/**************************************************************************
	 ********************************* Data Import *****************************
	 **************************************************************************/

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param size
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @throws SQLException
	 * @see #initRandomSubset(int, String, String)
	 */
	public void defineRandomSubset(int size, String subsetTable, String supersetTable, String comment)
			throws SQLException {
		createSubsetTable(subsetTable, supersetTable, comment);
		initRandomSubset(size, subsetTable, supersetTable);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param size
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @param schemaName
	 * @throws SQLException
	 * @see #initRandomSubset(int, String, String, String)
	 */
	public void defineRandomSubset(int size, String subsetTable, String supersetTable, String comment,
			String schemaName) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, schemaName, comment);
		initRandomSubset(size, subsetTable, supersetTable, schemaName);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param values
	 * @param subsetTable
	 * @param supersetTable
	 * @param columnToTest
	 * @param comment
	 * @throws SQLException
	 * @see #initSubset(List, String, String, String)
	 */
	public void defineSubset(List<String> values, String subsetTable, String supersetTable, String columnToTest,
			String comment) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, comment);
		initSubset(values, subsetTable, supersetTable, columnToTest);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param values
	 * @param subsetTable
	 * @param supersetTable
	 * @param columnToTest
	 * @param comment
	 * @param schemaName
	 * @throws SQLException
	 * @see #initSubset(List, String, String, String, String)
	 */
	public void defineSubset(List<String> values, String subsetTable, String supersetTable, String columnToTest,
			String comment, String schemaName) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, comment, schemaName);
		initSubset(values, subsetTable, supersetTable, columnToTest, schemaName);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @throws SQLException
	 * @see #initSubset(String, String)
	 */
	public void defineSubset(String subsetTable, String supersetTable, String comment) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, comment);
		initSubset(subsetTable, supersetTable);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @param schemaName
	 * @throws SQLException
	 * @see #initSubset(List, String, String, String, String)
	 */
	public void defineSubset(String subsetTable, String supersetTable, String comment, String schemaName)
			throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, comment, schemaName);
		initSubset(subsetTable, supersetTable, schemaName);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param conditionToCheck
	 * @param comment
	 * @throws SQLException
	 * @see #initSubsetWithWhereClause(String, String, String)
	 */
	public void defineSubsetWithWhereClause(String subsetTable, String supersetTable, String conditionToCheck,
			String comment) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, comment);
		initSubsetWithWhereClause(subsetTable, supersetTable, conditionToCheck);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param conditionToCheck
	 * @param comment
	 * @param schemaName
	 * @throws SQLException
	 * @see #initSubsetWithWhereClause(String, String, String, String)
	 */
	public void defineSubsetWithWhereClause(String subsetTable, String supersetTable, String conditionToCheck,
			String comment, String schemaName) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, comment, schemaName);
		initSubsetWithWhereClause(subsetTable, supersetTable, conditionToCheck, schemaName);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @throws SQLException
	 * @see #initMirrorSubset(String, String)
	 */
	public void defineMirrorSubset(String subsetTable, String supersetTable, boolean performUpdate, String comment)
			throws SQLException {
		createSubsetTable(subsetTable, supersetTable, comment);
		initMirrorSubset(subsetTable, supersetTable, performUpdate);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param maxNumberRefHops
	 *            the maximum number of times a foreign key reference to a data
	 *            table may be followed
	 * @param comment
	 * @throws SQLException
	 * @see #createSubsetTable(String, String, Integer, String)
	 * @see #initMirrorSubset(String, String)
	 */
	public void defineMirrorSubset(String subsetTable, String supersetTable, boolean performUpdate,
			Integer maxNumberRefHops, String comment) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, maxNumberRefHops, comment);
		initMirrorSubset(subsetTable, supersetTable, performUpdate);
	}

	/**
	 * <p>
	 * Convenience method for creating and initializing a subset in one step. See
	 * method references below for more information.
	 * </p>
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @param comment
	 * @param schemaName
	 * @throws SQLException
	 * @add {@link #defineMirrorSubset(String, String, String, String)}
	 */
	public void defineMirrorSubset(String subsetTable, String supersetTable, boolean performUpdate, String comment,
			String schemaName) throws SQLException {
		createSubsetTable(subsetTable, supersetTable, null, comment, schemaName);
		initMirrorSubset(subsetTable, supersetTable, performUpdate, schemaName);
	}

	/**
	 * 
	 * @see #initRandomSubset(int, String, String, String)
	 */
	public void initRandomSubset(int size, String subsetTable, String supersetTable) {
		initRandomSubset(size, subsetTable, supersetTable, activeTableSchema);
	}

	/**
	 * <p>
	 * Selects <code>size</code> rows of the given super set table randomly and
	 * inserts them into the subset table.
	 * </p>
	 * 
	 * @param size
	 *            size of the subset to create
	 * @param subsetTable
	 *            name of subset table to insert the chosen rows into
	 * @param superSetTable
	 *            name of the table to choose from
	 * @param schemaName
	 *            name of the schema to use
	 */
	public void initRandomSubset(int size, String subsetTable, String superSetTable, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);
		Connection conn = getConn();
		String sql = "INSERT INTO " + subsetTable + " (SELECT %s FROM " + superSetTable + " ORDER BY RANDOM() LIMIT "
				+ size + ");";
		sql = String.format(sql, fieldConfig.getPrimaryKeyString());
		try {
			conn.createStatement().execute(sql);
		} catch (SQLException e) {
			LOG.error(sql);
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// TODO: could be merged with defineSubsetWithWhereClause ?
	// EF: But here the ID list is broken down into smaller lists for which the
	// where clause is built. defineSubsetWithWhereClause isn't capable of such
	// things. So my vote is to let it the current way (09.01.2012).
	/**
	 * Defines a subset by populating a subset table with primary keys from another
	 * table. A WHERE clause is used to control which entries are copied, checking
	 * if columnToTest has the desired value.
	 * 
	 * @param values
	 *            Desired values for the columnToTest
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @param columnToTest
	 *            column to check for value
	 */
	public void initSubset(List<String> values, String subsetTable, String supersetTable, String columnToTest) {
		initSubset(values, subsetTable, supersetTable, columnToTest, activeTableSchema);
	}

	/**
	 * Defines a subset by populating a subset table with primary keys from another
	 * table. A WHERE clause is used to control which entries are copied, checking
	 * if columnToTest has the desired value.
	 * 
	 * @param values
	 *            Desired values for the columnToTest
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @param schemaName
	 *            schema to use
	 * @param columnToTest
	 *            column to check for value
	 */
	public void initSubset(List<String> values, String subsetTable, String supersetTable, String columnToTest,
			String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		int idSize = values.size();
		Connection conn = getConn();
		Statement st;
		String sql = null;
		try {
			st = conn.createStatement();
			for (int i = 0; i < idSize; i += ID_SUBLIST_SIZE) {
				List<String> subList = i + ID_SUBLIST_SIZE - 1 < idSize ? values.subList(i, i + ID_SUBLIST_SIZE)
						: values.subList(i, idSize);
				String expansionString = columnToTest + " = %s";
				if (fieldConfig.isOfStringType(columnToTest))
					;
				expansionString = columnToTest + " = '%s'";
				String[] expandedIDs = JulieXMLTools.expandArrayEntries(subList, expansionString);
				String where = StringUtils.join(expandedIDs, " OR ");
				sql = "INSERT INTO " + subsetTable + " (SELECT " + fieldConfig.getPrimaryKeyString() + " FROM "
						+ supersetTable + " WHERE " + where + ")";
				st.execute(sql);
			}
		} catch (SQLException e) {
			LOG.error("SQLError while initializing subset {}. SQL query was: {}", subsetTable, sql);
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Defines a subset by populating a subset table with primary keys from another
	 * table.
	 * 
	 * @param subsetTable
	 * @param supersetTable
	 * @see #initSubset(String, String, String)
	 */
	public void initSubset(String subsetTable, String supersetTable) {
		initSubset(subsetTable, supersetTable, activeTableSchema);
	}

	/**
	 * Defines a subset by populating a subset table with all primary keys from
	 * another table.
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @param schemaName
	 *            name of the schema used to determine the primary keys
	 */
	public void initSubset(String subsetTable, String supersetTable, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		if (fieldConfig.getPrimaryKey().length == 0)
			throw new IllegalStateException("Not subset tables corresponding to table scheme \"" + fieldConfig.getName()
					+ "\" can be created since this scheme does not define a primary key.");

		Connection conn = getConn();
		try {
			String pkStr = fieldConfig.getPrimaryKeyString();

			Statement st = conn.createStatement();
			String stStr = String.format("INSERT INTO %s (%s) (SELECT %s FROM %s);", subsetTable, pkStr, pkStr,
					supersetTable);
			st.execute(stStr);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Defines a subset by populating a subset table with primary keys from another
	 * table. All those entries are selected, for which the conditionToCheck is
	 * true.
	 * 
	 * @see #initSubsetWithWhereClause(String, String, String, String)
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @param whereClause
	 *            condition to check by a SQL WHERE clause, e.g. 'foo > 10'
	 */
	public void initSubsetWithWhereClause(String subsetTable, String supersetTable, String whereClause) {
		initSubsetWithWhereClause(subsetTable, supersetTable, whereClause, activeTableSchema);
	}

	/**
	 * Defines a subset by populating a subset table with primary keys from another
	 * table. All those entries are selected, for which the conditionToCheck is
	 * true.
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @param schemaName
	 *            name of the schema used to determine the primary keys
	 * @param whereClause
	 *            condition to check by a SQL WHERE clause, e.g. 'foo > 10'
	 */
	public void initSubsetWithWhereClause(String subsetTable, String supersetTable, String whereClause,
			String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		Connection conn = getConn();
		String stStr = null;
		try {
			if (!whereClause.toUpperCase().startsWith("WHERE"))
				whereClause = "WHERE " + whereClause;

			String pkStr = fieldConfig.getPrimaryKeyString();

			Statement st = conn.createStatement();
			stStr = String.format("INSERT INTO %s (%s) (SELECT %s FROM %s %s);", subsetTable, pkStr, pkStr,
					supersetTable, whereClause);
			st.execute(stStr);
		} catch (SQLException e) {
			LOG.error(stStr);
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void initMirrorSubset(String subsetTable, String supersetTable, boolean performUpdate) throws SQLException {
		initMirrorSubset(subsetTable, supersetTable, performUpdate, activeTableSchema);
	}

	/**
	 * Defines a mirror subset populating a subset table with primary keys from
	 * another table. <br>
	 * Its name is saved into a special meta data table to enable automatic syncing
	 * (changes to the superset are propagated to the mirror subset).
	 * 
	 * @param subsetTable
	 *            name of the subset table
	 * @param supersetTable
	 *            name of table to reference
	 * @throws SQLException
	 */
	public void initMirrorSubset(String subsetTable, String supersetTable, boolean performUpdate, String schemaName)
			throws SQLException {
		// TODO if the supersetTable is actually a subset table, we must
		// determine the correct schema of the data table which will eventually
		// be referenced and create/insert into the mirrorTable there! Currently
		// the mirrorTable can be located in the wrong places.
		// table listing mirror tables
		String mirrorTableName = getMirrorCollectionTableName(supersetTable);
		if (!subsetTable.contains("."))
			subsetTable = dbConfig.getActivePGSchema().concat(".").concat(subsetTable);

		// Create the mirror table list if not existing.
		if (!tableExists(mirrorTableName)) {
			List<String> columns = new ArrayList<String>();
			columns.add(Constants.MIRROR_COLUMN_DATA_TABLE_NAME + " text");
			columns.add(Constants.MIRROR_COLUMN_SUBSET_NAME + " text");
			columns.add(Constants.MIRROR_COLUMN_DO_RESET + " boolean DEFAULT true");
			columns.add(String.format("CONSTRAINT %s_pkey PRIMARY KEY (%s)", mirrorTableName.replace(".", ""),
					Constants.MIRROR_COLUMN_SUBSET_NAME));
			createTable(mirrorTableName, columns,
					"This table disposes the names of subset tables which mirror the data table " + supersetTable
							+ ". These subset tables will be updated as " + supersetTable
							+ " will obtains updates (insertions as well as deletions).");
		}
		// Create the actual subset and fill it to contain all primary key
		// values of the data table.
		initSubset(subsetTable, supersetTable, schemaName);
		Connection conn = getConn();

		// Add the new subset table to the list of mirror subset tables.
		String sql = null;
		try {
			Statement st = conn.createStatement();
			sql = String.format("INSERT INTO %s VALUES ('%s','%s',%b)", mirrorTableName, supersetTable, subsetTable,
					performUpdate);
			st.execute(sql);
		} catch (SQLException e) {
			LOG.error("Error executing SQL command: " + sql, e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param tableName
	 *            table to gather mirror subsets for
	 * @return names of all mirror subsets for this table
	 */
	private LinkedHashMap<String, Boolean> getMirrorSubsetNames(Connection conn, String tableName) {
		String mirrorTableName = getMirrorCollectionTableName(tableName);
		if (!tableExists(conn, mirrorTableName))
			return null;

		// The mirror tables are inserted into the collecting table with schema
		// information. If the given data table is not qualified, we assume it
		// to be in the same postgres scheme as the looked-up mirror subset
		// collection table. And that is - for unqualified data tables - the
		// active postgres scheme given in the configuration file (see
		// 'getMirrorCollectionTableName' on how the mirror subset collection
		// table name is determined).
		if (!tableName.contains("."))
			tableName = dbConfig.getActivePGSchema() + "." + tableName;

		LinkedHashMap<String, Boolean> mirrorSubsetList = new LinkedHashMap<>();

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(String.format(
					"SELECT %s,%s FROM %s WHERE " + Constants.MIRROR_COLUMN_DATA_TABLE_NAME + "='%s'",
					Constants.MIRROR_COLUMN_SUBSET_NAME, Constants.MIRROR_COLUMN_DO_RESET, mirrorTableName, tableName));
			while (rs.next()) {
				String mirrorTable = rs.getString(1);
				Boolean performUpdate = rs.getBoolean(2);
				String refDataTable = getReferencedTable(mirrorTable);
				if (refDataTable != null && refDataTable.equals(tableName))
					mirrorSubsetList.put(mirrorTable, performUpdate);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return mirrorSubsetList;
	}

	/**
	 * Determines the (qualified) name of the meta data table listing all mirror
	 * subsets.
	 * 
	 * @param tableName
	 *            name (and perhaps schema) of a table
	 * @return name of a table listing mirror subsets
	 */
	private String getMirrorCollectionTableName(String tableName) {
		String[] dataTablePath = tableName.split("\\.");
		String dataTableSchema = null;
		if (dataTablePath.length > 1)
			dataTableSchema = dataTablePath[0];
		return dataTableSchema != null ? dataTableSchema + "." + Constants.MIRROR_COLLECTION_NAME
				: getActiveDataPGSchema() + "." + Constants.MIRROR_COLLECTION_NAME;
	}

	/**
	 * Sets the values in the <code>is_processed</code>, <code>is_in_process</code>,
	 * <code>has_errors</code> and <code>log</code> columns of a subset to
	 * <code>FALSE</code>.
	 * 
	 * @param subsetTableName
	 *            name of the subset to reset
	 */
	public void resetSubset(String subsetTableName) {
		resetSubset(subsetTableName, false, false, null);
	}

	/**
	 * Sets the values in the <code>is_processed</code>, <code>is_in_process</code>,
	 * <code>has_errors</code> and <code>log</code> columns of a subset to
	 * <code>FALSE</code> where the corresponding rows are
	 * <code>is_in_process</code> or <code>is_processed</code>.
	 * <p>
	 * The boolean parameter <code>whereNotProcessed</code> is used for the use case
	 * where only those rows should be reset that are <code>in_process</code> but
	 * not <code>is_processed</code> which may happen when a pipeline crashed, a
	 * document has errors or a pipeline ist just canceled.
	 * </p>
	 * <p>
	 * In a similar fashion, <code>whereNoErrors</code> resets those rows that have
	 * no errors.
	 * </p>
	 * <p>
	 * Both boolean parameters may be combined in which case only non-processed rows
	 * without errors will be reset.
	 * </p>
	 * 
	 * @param subsetTableName
	 *            name of the table to reset unprocessed rows
	 */
	public void resetSubset(String subsetTableName, boolean whereNotProcessed, boolean whereNoErrors,
			String lastComponent) {
		Connection conn = getConn();
		String stStr = null;
		try {
			List<String> constraints = new ArrayList<>();
			if (whereNotProcessed)
				constraints.add(Constants.IS_PROCESSED + " = FALSE");
			if (whereNoErrors)
				constraints.add(Constants.HAS_ERRORS + " = FALSE");
			if (lastComponent != null)
				constraints.add(Constants.LAST_COMPONENT + " = '" + lastComponent + "'");
			Statement st = conn.createStatement();
			stStr = String.format(
					"UPDATE %s SET %s = FALSE, %s = FALSE, %s='%s', %s = FALSE, %s = NULL, %s = NULL WHERE (%s = TRUE OR %s = TRUE)",
					subsetTableName, Constants.IN_PROCESS, Constants.IS_PROCESSED, Constants.LAST_COMPONENT,
					DEFAULT_PIPELINE_STATE, Constants.HAS_ERRORS, Constants.LOG, Constants.PROCESSING_TIMESTAMP,
					Constants.IS_PROCESSED, Constants.IN_PROCESS);
			if (!constraints.isEmpty())
				stStr += " AND " + constraints.stream().collect(Collectors.joining(" AND "));
			st.execute(stStr);
		} catch (SQLException e) {
			LOG.error("Error executing SQL command: " + stStr, e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @see #resetSubset(String, Collection, String)
	 * @param subsetTableName
	 * @param pkValues
	 * @return
	 */
	public int[] resetSubset(String subsetTableName, List<Object[]> pkValues) {
		return resetSubset(subsetTableName, pkValues, activeTableSchema);
	}

	/**
	 * 
	 * @param pkValues
	 * @param sqlFormatString
	 * @param schemaName
	 * @return
	 * @see #resetSubset(String, List, String)
	 */
	public int[] performBatchUpdate(List<Object[]> pkValues, String sqlFormatString, String schemaName) {

		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		Connection conn = getConn();
		String stStr = null;
		List<Integer> resultList = new ArrayList<>();
		try {
			conn.setAutoCommit(false);
			String whereArgument = StringUtils.join(fieldConfig.expandPKNames("%s = ?"), " AND ");
			stStr = String.format(sqlFormatString, whereArgument);

			LOG.trace("Performing batch update with SQL command: {}", stStr);

			PreparedStatement ps = conn.prepareStatement(stStr);
			int i = 0;
			for (Object[] id : pkValues) {
				for (int j = 0; j < id.length; ++j) {
					setPreparedStatementParameterWithType(j + 1, ps, id[j], fieldConfig.getPrimaryKey()[j],
							fieldConfig);
				}
				ps.addBatch();

				if (i >= commitBatchSize) {
					int[] results = ps.executeBatch();
					for (int result : results)
						resultList.add(result);
					conn.commit();
					ps.clearBatch();
					i = 0;
				}
				++i;
			}
			int[] results = ps.executeBatch();
			for (int result : results)
				resultList.add(result);
			conn.commit();

		} catch (SQLException e) {
			LOG.error("Error executing SQL command: " + stStr, e);
		} finally {
			try {
				conn.setAutoCommit(true);
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int[] ret = new int[resultList.size()];
		for (int i = 0; i < ret.length; i++)
			ret[i] = resultList.get(i);
		return ret;
	}

	/**
	 * Sets the values in the <code>is_processed</code> and
	 * <code>is_in_process</code> rows of a subset to <code>FALSE</code>. Only
	 * resets the subset table rows where the primary key equals one of the entries
	 * in <code>pkValues</code>.
	 * 
	 * @param subsetTableName
	 *            - name of the table to reset
	 * @param pkValues
	 *            - list of primary keys
	 * @return
	 */
	public int[] resetSubset(String subsetTableName, List<Object[]> pkValues, String schemaName) {
		// We intentionally do not check whether the rows are already reset
		// because we want the only reason for the update to not affect a
		// row to be that the row doesn't exist.
		// The original where was: 'where (is_processed = TRUE OR
		// is_in_process = TRUE) AND %s'
		String updateFormatString = "UPDATE " + subsetTableName + " SET " + Constants.IS_PROCESSED + "=FALSE, "
				+ Constants.IN_PROCESS + "= FALSE, " + Constants.LAST_COMPONENT + "='" + DEFAULT_PIPELINE_STATE
				+ "' WHERE %s";
		return performBatchUpdate(pkValues, updateFormatString, schemaName);
	}

	public int[] determineExistingSubsetRows(String subsetTableName, List<Object[]> pkValues, String schemaName) {
		String updateFormatString = "UPDATE " + subsetTableName + " SET has_errors = has_errors " + "where %s";
		return performBatchUpdate(pkValues, updateFormatString, schemaName);
	}

	/**
	 * @see #importFromXML(Iterable, String, String, String)
	 * @param xmls
	 * @param tableName
	 * @param identifier
	 */
	public void importFromXML(Iterable<byte[]> xmls, String identifier, String tableName) {
		importFromXML(xmls, tableName, identifier, activeTableSchema);
	}

	/**
	 * Imports XMLs into a table.
	 * 
	 * @param xmls
	 *            - an Iterator over XMLs as byte[]
	 * @param tableName
	 *            - name of the table to import
	 * @param identifier
	 *            - used for error messages
	 */
	public void importFromXML(Iterable<byte[]> xmls, String tableName, String identifier, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		for (byte[] xml : xmls) {
			Iterator<Map<String, Object>> it = JulieXMLTools.constructRowIterator(xml, BUFFER_SIZE,
					fieldConfig.getForEachXPath(), fieldConfig.getFields(), identifier);
			importFromRowIterator(it, tableName);
		}
	}

	/**
	 * Import new medline XMLs in a existing table from an XML file or a directory
	 * of XML files. The XML must be in MEDLINE XML format and can additionally be
	 * (G)Zipped.
	 * 
	 * @see #importFromXMLFile(String, String, String)
	 * @param fileStr
	 *            - path to file or directory of (G)Zipped MEDLINE XML file(s)
	 * @param tableName
	 *            - name of the target table
	 */
	public void importFromXMLFile(String fileStr, String tableName) {
		importFromXMLFile(fileStr, tableName, activeTableSchema);
	}

	/**
	 * Import new medline XMLs in a existing table from an XML file or a directory
	 * of XML files. The XML must be in MEDLINE XML format and can additionally be
	 * (G)Zipped.
	 * 
	 * @param fileStr
	 *            - path to file or directory of (G)Zipped MEDLINE XML file(s)
	 * @param tableName
	 *            - name of the target table
	 * @param schemaName
	 *            the table schema to use for the import
	 */
	public void importFromXMLFile(String fileStr, String tableName, String schemaName) {
		LOG.info("Starting import...");

		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		String[] fileNames;
		File fileOrDir = new File(fileStr);
		if (!fileOrDir.isDirectory()) {
			fileNames = new String[1];
			fileNames[0] = fileStr;
		} else {
			fileNames = fileOrDir.list(new FilenameFilter() {
				public boolean accept(File arg0, String arg1) {
					// TODO write accepted file extensions into config
					return arg1.endsWith(".zip") || arg1.endsWith(".gz") || arg1.endsWith(".xml");
				}
			});
		}
		// medline files are sorted chronological
		Arrays.sort(fileNames);
		XMLPreparer xp = new XMLPreparer(fileOrDir, fieldConfig);
		for (String fileName : fileNames) {
			LOG.info("Importing " + fileName);
			Iterator<Map<String, Object>> it = xp.prepare(fileName);
			importFromRowIterator(it, tableName, null, true, schemaName);
		}
	}

	/**
	 * @see #updateFromXML(String, String)
	 * @param fileStr
	 * @param tableName
	 */
	public void updateFromXML(String fileStr, String tableName) {
		updateFromXML(fileStr, tableName, activeTableSchema);
	}

	/**
	 * Updates an existing database. If the file contains new entries those are
	 * inserted, otherwise the table is updated to the version in the file.
	 * 
	 * @param fileStr
	 *            - file containing new or updated entries
	 * @param tableName
	 *            - table to update
	 */
	public void updateFromXML(String fileStr, String tableName, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		// TODO deprecated way of determining the primary key fields?! Make sure
		// and use appropriate method of FieldConfig.
		List<String> pks = new ArrayList<String>();
		List<Map<String, String>> fields = fieldConfig.getFields();
		for (Map<String, String> field : fields)
			if (field.containsKey("primaryKey"))
				if (field.get("primaryKey").equals(true))
					pks.add(field.get("name"));
		LOG.info("Starting update...");

		String[] fileNames;
		File fileOrDir = new File(fileStr);
		if (!fileOrDir.isDirectory()) {
			fileNames = new String[1];
			fileNames[0] = fileStr;
		} else {
			fileNames = fileOrDir.list(new FilenameFilter() {
				public boolean accept(File arg0, String arg1) {
					// TODO write accepted file extensions in configuration
					// file
					return arg1.endsWith(".zip") || arg1.endsWith(".gz") || arg1.endsWith(".xml");
				}
			});
		}

		// in medline, the files are ordered chronological
		Arrays.sort(fileNames);
		XMLPreparer xp = new XMLPreparer(fileOrDir, fieldConfig);
		for (String fileName : fileNames) {
			LOG.info("Updating from " + fileName);
			Iterator<Map<String, Object>> fileIt = xp.prepare(fileName);
			updateFromRowIterator(fileIt, tableName, null, true, schemaName);
		}
	}

	/**
	 * @see #importFromRowIterator(Iterator, String, String)
	 * @param it
	 * @param tableName
	 */
	public void importFromRowIterator(Iterator<Map<String, Object>> it, String tableName) {
		importFromRowIterator(it, tableName, null, true, activeTableSchema);
	}

	/**
	 * Internal method to import into an existing table
	 * 
	 * @param it
	 *            - an Iterator, yielding rows to insert into the database
	 * 
	 * @param tableName
	 *            - the updated table
	 * @param externalConn
	 *            - if not <tt>null</tt>, this connection will be employed instead
	 *            of asking for a new connection
	 * @param commit
	 *            - if <tt>true</tt>, the inserted data will be committed in batches
	 *            within this method; no commits will happen otherwise.
	 * @param schemaName
	 *            the name of the table schema corresponding to the data table
	 */
	public void importFromRowIterator(Iterator<Map<String, Object>> it, String tableName, Connection externalConn,
			boolean commit, String schemaName) {
		// Fast return to spare some unnecessary communication with the
		// database.
		if (!it.hasNext())
			return;

		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		String dataImportStmtString = constructImportStatementString(tableName, fieldConfig);
		String mirrorUpdateStmtString = constructMirrorInsertStatementString(fieldConfig);
		Connection conn = null != externalConn ? externalConn : getConn();
		try {
			// Get the list of mirror subsets in which all new primary keys must
			// be inserted as well.
			LinkedHashMap<String, Boolean> mirrorNames = getMirrorSubsetNames(conn, tableName);

			if (null == externalConn)
				conn.setAutoCommit(false);
			PreparedStatement psDataImport = conn.prepareStatement(dataImportStmtString);
			/*
			 * PreparedStatement psMirrorUpdate = conn
			 * .prepareStatement(mirrorUpdateStmtString);
			 */

			List<PreparedStatement> mirrorStatements = null;
			if (mirrorNames != null) {
				mirrorStatements = new ArrayList<PreparedStatement>();
				for (String mirror : mirrorNames.keySet()) {
					mirrorStatements.add(conn.prepareStatement(String.format(mirrorUpdateStmtString, mirror)));
				}
			}
			List<Map<String, String>> fields = fieldConfig.getFields();
			int i = 0;
			while (it.hasNext()) {
				Map<String, Object> row = it.next();
				for (int j = 0; j < fields.size(); j++) {
					Map<String, String> field = fields.get(j);
					String fieldName = field.get(JulieXMLConstants.NAME);
					setPreparedStatementParameterWithType(j + 1, psDataImport, row.get(fieldName), fieldName,
							fieldConfig);
				}
				psDataImport.addBatch();

				if (mirrorStatements != null) {
					for (PreparedStatement ps : mirrorStatements) {
						for (int j = 0; j < fieldConfig.getPrimaryKey().length; j++) {
							String fieldName = fieldConfig.getPrimaryKey()[j];
							setPreparedStatementParameterWithType(j + 1, ps, row.get(fieldName), fieldName,
									fieldConfig);
						}
						ps.addBatch();
					}
				}

				++i;
				if (i >= commitBatchSize) {
					psDataImport.executeBatch();
					if (mirrorStatements != null)
						for (PreparedStatement ps : mirrorStatements)
							ps.executeBatch();
					// NOTE If a fast return from a commit is required, rather
					// use
					// Postgres asynchroneous commit
					// (http://www.postgresql.org/docs/9.1/static/wal-async-commit.html)
					// commit(conn);
					if (commit)
						conn.commit();
					psDataImport = conn.prepareStatement(dataImportStmtString);
					i = 0;
				}
			}
			if (i > 0) {
				psDataImport.executeBatch();
				if (commit)
					conn.commit();
				if (mirrorStatements != null)
					for (PreparedStatement ps : mirrorStatements)
						ps.executeBatch();
				// NOTE If a fast return from a commit is required, rather
				// use
				// Postgres asynchroneous commit
				// (http://www.postgresql.org/docs/9.1/static/wal-async-commit.html)
				// commit(conn);
				if (commit)
					conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			SQLException nextException = e.getNextException();
			if (nextException != null) {
				LOG.error("Next exception: ", nextException);
			}
		} finally {
			try {
				if (commitThread != null)
					commitThread.join();
				if (null == externalConn)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * <p>
	 * Updates a table with the entries yielded by the iterator. If the entries is
	 * not yet in the table, it will be inserted instead.
	 * </p>
	 * <p>
	 * The input rows are expected to fit the active table schema.
	 * </p>
	 * 
	 * @param it
	 *            - an Iterator, yielding new or updated entries.
	 * @param tableName
	 *            - the updated table
	 */
	public void updateFromRowIterator(Iterator<Map<String, Object>> it, String tableName) {
		updateFromRowIterator(it, tableName, null, true, activeTableSchema);
	}

	/**
	 * <p>
	 * Updates a table with the entries yielded by the iterator. If the entries is
	 * not yet in the table, it will be inserted instead.
	 * </p>
	 * <p>
	 * The input rows are expected to fit the table schema <code>schemaName</code>.
	 * </p>
	 * 
	 * @param it
	 *            - an Iterator, yielding new or updated entries.
	 * @param tableName
	 *            - the updated table
	 */

	/**
	 * 
	 * <p>
	 * Updates a table with the entries yielded by the iterator. If the entries is
	 * not yet in the table, it will be inserted instead.
	 * </p>
	 * <p>
	 * The input rows are expected to fit the table schema <code>schemaName</code>.
	 * 
	 * @param it
	 *            - an Iterator, yielding new or updated entries.
	 * @param tableName
	 *            - the updated table
	 * @param externalConn
	 *            - if not <tt>null</tt>, this connection will be employed instead
	 *            of asking for a new connection
	 * @param commit
	 *            - if <tt>true</tt>, the updated data will be committed in batches
	 *            within this method; nothing will be commit otherwise.
	 * @param schemaName
	 *            the name of the table schema corresponding to the updated data
	 *            table
	 */
	public void updateFromRowIterator(Iterator<Map<String, Object>> it, String tableName, Connection externalConn,
			boolean commit, String schemaName) {
		// Fast return to avoid unnecessary communication with the database.
		if (!it.hasNext())
			return;

		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		String statementString = constructUpdateStatementString(tableName, fieldConfig);
		String mirrorInsertStmtString = constructMirrorInsertStatementString(fieldConfig);
		Connection conn = null != externalConn ? externalConn : getConn();
		try {
			LinkedHashMap<String, Boolean> mirrorNames = getMirrorSubsetNames(conn, tableName);

			List<PreparedStatement> mirrorStatements = null;
			if (mirrorNames != null) {
				mirrorStatements = new ArrayList<PreparedStatement>();
				for (String mirror : mirrorNames.keySet()) {
					mirrorStatements.add(conn.prepareStatement(String.format(mirrorInsertStmtString, mirror)));
				}
			}

			if (null == externalConn)
				conn.setAutoCommit(false);
			int i = 0;
			PreparedStatement ps = conn.prepareStatement(statementString);
			List<Map<String, String>> fields = fieldConfig.getFields();
			String[] primaryKey = fieldConfig.getPrimaryKey();

			// Set<String> alreadyUpdatedIds = new HashSet<>();
			int numAlreadyExisted = 0;
			// This map will assemble for each primary key only the NEWEST (in
			// XML the latest in Medline) row. Its size is an approximation of
			// Medline blob XML files.
			// TODO we should actually check for the PMID version and take the
			// highest
			Map<String, Map<String, Object>> rowsByPk = new HashMap<>(commitBatchSize * 10);
			while (it.hasNext()) {
				Map<String, Object> row = it.next();
				StringBuilder rowPrimaryKey = new StringBuilder();
				for (int j = 0; j < primaryKey.length; j++) {
					String keyFieldName = primaryKey[j];
					Object key = row.get(keyFieldName);
					rowPrimaryKey.append(key);

				}
				String pk = rowPrimaryKey.toString();
				if (rowsByPk.containsKey(pk))
					++numAlreadyExisted;
				rowsByPk.put(pk, row);
			}

			ArrayList<Map<String, Object>> cache = new ArrayList<Map<String, Object>>(commitBatchSize);
			// while (it.hasNext()) {
			// Map<String, Object> row = it.next();
			for (Map<String, Object> row : rowsByPk.values()) {
				// StringBuilder rowPrimaryKey = new StringBuilder();

				for (int j = 0; j < fields.size() + primaryKey.length; j++) {
					// for (int j = 0; j < fields.size(); j++) {
					if (j < fields.size()) {
						Map<String, String> field = fields.get(j);
						String fieldName = field.get(JulieXMLConstants.NAME);
						setPreparedStatementParameterWithType(j + 1, ps, row.get(fieldName), null, null);
					} else {
						String key = primaryKey[j - fields.size()];
						Object keyValue = row.get(key);
						// rowPrimaryKey.append(keyValue);
						setPreparedStatementParameterWithType(j + 1, ps, keyValue, null, null);
					}
				}
				// if (alreadyUpdatedIds.add(rowPrimaryKey.toString())) {
				ps.addBatch();
				cache.add(row);

				// } else {
				// ps.clearParameters();
				// LOG.warn(
				// "Primary key {} exists multiple times in one batch of data.
				// All but the first occurence will be ignored.",
				// rowPrimaryKey.toString());
				// numAlreadyExisted++;
				// }

				++i;
				if (i >= commitBatchSize) {
					LOG.trace("Committing batch of size {}", i);
					executeAndCommitUpdate(tableName, externalConn != null ? externalConn : conn, commit, schemaName, fieldConfig, mirrorNames,
							mirrorStatements, ps, cache);
					cache.clear();
					i = 0;
				}
			}
			if (i > 0) {
				LOG.trace("Commiting last batch of size {}", i);
				executeAndCommitUpdate(tableName, externalConn != null ? externalConn : conn, commit, schemaName, fieldConfig, mirrorNames,
						mirrorStatements, ps, cache);
			}
			// LOG.info(
			// "Updated {} documents. {} documents were skipped because there
			// existed documents with same primary keys multiple times in the
			// data.",
			// alreadyUpdatedIds.size(), numAlreadyExisted);
			LOG.info(
					"Updated {} documents. {} documents were skipped because there existed documents with same primary keys multiple times in the data. In those cases, the last occurrence of the document was inserted into the database",
					rowsByPk.size(), numAlreadyExisted);
		} catch (SQLException e) {
			LOG.error(
					"SQL error while updating table {}. Database configuration is: {}. Table schema configuration is: {}",
					new Object[] { tableName, dbConfig, fieldConfig });
			e.printStackTrace();
			SQLException nextException = e.getNextException();
			if (null != nextException) {
				LOG.error("Next exception was: ", nextException);
				nextException.printStackTrace();
			}
		} finally {
			try {
				if (commitThread != null)
					commitThread.join();
				if (null == externalConn)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Performs the actual update in the database. Additionally manages the
	 * appropriate reset of rows in mirror subsets and the addition of missing rows
	 * in mirror subsets.
	 * 
	 * @param tableName
	 * @param externalConn
	 * @param commit
	 * @param schemaName
	 * @param fieldConfig
	 * @param mirrorNames
	 * @param mirrorStatements
	 * @param ps
	 * @param cache
	 * @throws SQLException
	 */
	private void executeAndCommitUpdate(String tableName, Connection externalConn, boolean commit, String schemaName,
			FieldConfig fieldConfig, LinkedHashMap<String, Boolean> mirrorNames,
			List<PreparedStatement> mirrorStatements, PreparedStatement ps, ArrayList<Map<String, Object>> cache)
			throws SQLException {
		int[] returned = ps.executeBatch();

		List<Map<String, Object>> toInsert = new ArrayList<Map<String, Object>>(commitBatchSize);
		List<Map<String, Object>> toResetRows = new ArrayList<Map<String, Object>>(commitBatchSize);
		List<Object[]> toResetPKs = new ArrayList<Object[]>();

		fillUpdateLists(cache, returned, toInsert, toResetPKs, toResetRows, fieldConfig);
		importFromRowIterator(toInsert.iterator(), tableName, externalConn, commit, schemaName);
		// Do a commit to end the transaction. This is sometimes even necessary
		// because following transactions would be blocked otherwise.
		LOG.trace("Committing updates to the data table.");
		externalConn.commit();
		if (mirrorNames != null) {
			LOG.trace("Applying updates to mirror subsets:");
			List<Map<String, Object>> toInsertMirror = new ArrayList<Map<String, Object>>(commitBatchSize);
			Iterator<String> mirrorNamesIt = mirrorNames.keySet().iterator();
			Iterator<PreparedStatement> mirrorStatementsIt = mirrorStatements.iterator();
			for (int j = 0; j < mirrorNames.size(); j++) {
				String mirrorName = mirrorNamesIt.next();
				LOG.trace("Applying to mirror subset \"{}\"", mirrorName);
				// The mirrorNames hashmap has as values booleans telling
				// whether to reset a mirror table or not. If not, we still want
				// to know whether there are any missing rows and insert them.
				if (mirrorNames.get(mirrorName)) {
					LOG.trace("Resetting updated rows.");
					returned = resetSubset(mirrorName, toResetPKs, schemaName);
				} else {
					LOG.trace("Updates rows are NOT reset.");
					returned = determineExistingSubsetRows(mirrorName, toResetPKs, schemaName);
				}
				// Possibly some update documents don't even exist
				// in a mirror subset. This shouldn't happen of
				// course, but it might due to errors. This allows
				// to repair the error by an update instead of
				// deleting the missing data from the data table and
				// re-import it.
				fillUpdateLists(toResetRows, returned, toInsertMirror, null, null, fieldConfig);
				if (toInsertMirror.size() > 0) {
					LOG.trace("{} updated rows where not found in this mirror subset. They will be added");
					// The mirror insert statements are a parallel list
					// to mirrorNames, thus the jth mirrorName belong to
					// the jth insert statement.
					PreparedStatement mirrorPS = mirrorStatementsIt.next();
					for (Map<String, Object> missingMirrorRow : toInsertMirror) {
						for (int k = 0; k < fieldConfig.getPrimaryKey().length; k++) {
							String fieldName = fieldConfig.getPrimaryKey()[k];
							setPreparedStatementParameterWithType(k + 1, mirrorPS, missingMirrorRow.get(fieldName),
									fieldName, fieldConfig);
						}
						mirrorPS.addBatch();
					}
					mirrorPS.executeBatch();
					toInsertMirror.clear();
				} else {
					LOG.trace("All updated rows exist in the mirror subset.");
				}
			}
		}

		// commit(conn);
		if (commit) {
			LOG.trace("Committing updates.");
			externalConn.commit();
		}
	}

	/**
	 * <p>
	 * Prepares lists of documents to insert into a table and primary keys for which
	 * mirror subsets must be reseted because the respective documents in the data
	 * table have been updated. The preparation happens basing on the return value
	 * of an SQL operation trying to operate on a set of documents, e.g. updating
	 * them. A batch UPDATE command, for instance, returns an int[] where for each
	 * batch item 0 indicates non-success (could not be updated, presumably because
	 * the primary key in the update command does not exist) and 1 indicates
	 * success.<br/>
	 * Successful updated documents must be reseted in the mirror subsets, documents
	 * that could not be updated (and thus don't exist) must be inserted.
	 * </p>
	 * 
	 * @param cache
	 *            Input: The list of rows for which the original SQL command was
	 *            issued that returned the values in <tt>returned</tt>. Must be
	 *            parallel to <tt>returned</tt>.
	 * @param returned
	 *            Input: The return values of the SQL command issued on base of the
	 *            rows contained in <tt>cache</tt>.
	 * @param toInsert
	 *            Output: Rows from <tt>cache</tt> filtered by "corresponding value
	 *            in <tt>returned</tt> was &lt;= 0 (non-success)".
	 * @param toResetPKs
	 *            Output: Primary keys from <tt>cache</tt> rows for which
	 *            <tt>returned</tt> holds a value &gt;0 (e.g. successful update).
	 * @param toResetRows
	 *            Output, may be null: The rows from <tt>cache</tt> for which
	 *            <tt>returned</tt> holds a value &gt;0.
	 * @param fieldConfig
	 *            Input: Field configuration to determine the correct primary key.
	 */
	private void fillUpdateLists(List<Map<String, Object>> cache, int[] returned, List<Map<String, Object>> toInsert,
			List<Object[]> toResetPKs, List<Map<String, Object>> toResetRows, FieldConfig fieldConfig) {
		for (int j = 0; j < returned.length; ++j) {
			Map<String, Object> newRow = cache.get(j);
			if (returned[j] <= 0) {
				toInsert.add(newRow);
			} else {
				if (null != toResetPKs) {
					Object[] pkValues = new Object[fieldConfig.getPrimaryKey().length];
					for (int k = 0; k < pkValues.length; k++) {
						String pkColumn = fieldConfig.getPrimaryKey()[k];
						pkValues[k] = newRow.get(pkColumn);
					}
					toResetPKs.add(pkValues);
				}
				if (null != toResetRows)
					toResetRows.add(newRow);
			}
		}
	}

	/**
	 * Constructs an SQL prepared statement for import of data rows into the
	 * database table <code>tableName</code> according to the field schema
	 * definition.
	 * <p>
	 * <samp> <b>Example:</b>
	 * <p>
	 * If the field schema contains two rows 'pmid' and 'xml', the statement
	 * expressions expects all these rows to be filled. The resulting String will be
	 * <p>
	 * <center>INSERT INTO <tableName> (pmid,xml) VALUES (?,?)</center> </samp>
	 * 
	 * @param tableName
	 *            Name of the database table to import data into.
	 * @param fieldDefinition
	 *            A {@link FieldConfig} object determining the rows to be imported.
	 * @return An SQL prepared statement string for import of data into the table.
	 */
	private String constructImportStatementString(String tableName, FieldConfig fieldDefinition) {
		String stmtTemplate = "INSERT INTO %s (%s) VALUES (%s)";
		List<Map<String, String>> fields = fieldDefinition.getFields();
		StringBuilder columnsStrBuilder = new StringBuilder();
		StringBuilder valuesStrBuilder = new StringBuilder();
		for (int i = 0; i < fields.size(); ++i) {
			columnsStrBuilder.append(fields.get(i).get(JulieXMLConstants.NAME));
			valuesStrBuilder.append("?");
			if (i < fields.size() - 1) {
				columnsStrBuilder.append(",");
				valuesStrBuilder.append(",");
			}
		}
		return String.format(stmtTemplate, tableName, columnsStrBuilder.toString(), valuesStrBuilder.toString());
	}

	/**
	 * Creates an SQL-template, usable in prepared statements which add new values
	 * into a table
	 * 
	 * @param fieldConfig
	 *            - used to get the primary key, as the template must contain it
	 * @return - an SQL string for inserting, containing a '?' for every primary key
	 *         and a %s for the table name
	 */
	private String constructMirrorInsertStatementString(FieldConfig fieldConfig) {
		String stmtTemplate = "INSERT INTO %s (%s) VALUES (%s)";
		String pkStr = fieldConfig.getPrimaryKeyString();
		String[] wildCards = new String[fieldConfig.getPrimaryKey().length];
		for (int i = 0; i < wildCards.length; i++)
			wildCards[i] = "?";
		String wildCardStr = StringUtils.join(wildCards, ",");
		return String.format(stmtTemplate, "%s", pkStr, wildCardStr);
	}

	/**
	 * Constructs an SQL prepared statement for updating data rows in the database
	 * table <code>tableName</code> according to the field schema definition.
	 * <p>
	 * <samp> <b>Example:</b>
	 * <p>
	 * If the field schema contains two rows ('pmid' and 'xml') and pmid is primary
	 * key, the resulting String will be
	 * <p>
	 * <center>UPDATE <tableName> SET pmid=?, xml=? WHERE pmid=?</center> </samp>
	 * 
	 * @param tableName
	 *            Name of the database table to import data into.
	 * @param fieldDefinition
	 *            A {@link FieldConfig} object determining the rows to be imported.
	 * @return An SQL prepared statement string for import of data into the table.
	 */
	private String constructUpdateStatementString(String tableName, FieldConfig fieldDefinition) {
		String stmtTemplate = "UPDATE %s SET %s WHERE %s";
		List<Map<String, String>> fields = fieldDefinition.getFields();
		StringBuilder newValueStrBuilder = new StringBuilder();
		for (int i = 0; i < fields.size(); ++i) {
			newValueStrBuilder.append(fields.get(i).get(JulieXMLConstants.NAME)).append("=?");
			if (i < fields.size() - 1)
				newValueStrBuilder.append(",");
		}
		String[] primaryKeys = fieldDefinition.getPrimaryKey();
		StringBuilder conditionStrBuilder = new StringBuilder();
		for (int i = 0; i < primaryKeys.length; ++i) {
			String key = primaryKeys[i];
			conditionStrBuilder.append(key).append("=?");
			if (i < primaryKeys.length - 1)
				conditionStrBuilder.append(" AND ");
		}
		String statementString = String.format(stmtTemplate, tableName, newValueStrBuilder.toString(),
				conditionStrBuilder.toString());
		LOG.trace("PreparedStatement update command: {}", statementString);
		return statementString;
	}

	/**
	 * Used internal to commit the changes done by a connection in a seperate thread
	 * NOTE If a fast return from a commit is required, rather use Postgres
	 * asynchroneous commit
	 * (http://www.postgresql.org/docs/9.1/static/wal-async-commit.html)
	 */
	// private void commit(final Connection conn) throws InterruptedException {
	// if (commitThread != null)
	// commitThread.join();
	// commitThread = new Thread() {
	// public void run() {
	// try {
	// conn.commit();
	// } catch (SQLException e) {
	// e.printStackTrace();
	// }
	// }
	// };
	// commitThread.start();
	// }

	/**
	 * Alters an table, executing the supplied action
	 * 
	 * @param action
	 *            - SQL fragment, specifiying how to alter the table
	 * @param tableName
	 *            - table to alter
	 */
	private void alterTable(String action, String tableName) {
		Connection conn = getConn();
		String sqlString = "ALTER TABLE " + tableName + " " + action;
		try {
			Statement st = conn.createStatement();
			st.execute(sqlString);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * A class to parse xml files and make them accessible with an iterator
	 * 
	 * @author hellrich
	 * 
	 */
	private class XMLPreparer {
		private File fileOrDir;
		private final FieldConfig fieldConfig;

		protected XMLPreparer(File fileOrDir, FieldConfig fieldConfig) {
			this.fileOrDir = fileOrDir;
			this.fieldConfig = fieldConfig;
		}

		/**
		 * Parses a xml file according to the FieldConfig for this DatabaseConnector
		 * 
		 * @param fileName
		 *            - file to parse
		 * @return - an iterator, yielding rows for a database
		 */
		protected Iterator<Map<String, Object>> prepare(String fileName) {

			String xmlFilePath = fileOrDir.getAbsolutePath();
			if (fileOrDir.isDirectory()) {
				xmlFilePath = xmlFilePath + "/" + fileName;
			}
			File xmlFile = new File(xmlFilePath);
			boolean hugeFile = false;
			if (xmlFile.length() >= 1024 * 1024 * 1024) {
				LOG.info("File is larger than 1GB. Trying VTD huge.");
				hugeFile = true;
			}
			return JulieXMLTools.constructRowIterator(xmlFilePath, BUFFER_SIZE, fieldConfig.getForEachXPath(),
					fieldConfig.getFields(), hugeFile);

		}

	}

	/********************************
	 * Data Retrieval
	 ****************************************************************************************************/
	/*
	 * Speed: (tested by repeated queries, using a pool-pc and 1000 as batchSize)
	 * queryAll() fetched 8.5 documents/ms (33min for whole db with 16.9*10e6
	 * documents) query(ids) fetched 9.3 documents/ms (9.3sec for 10e5 documents of
	 * a PMID sample)
	 */

	/**
	 * @see #queryWithTime(List, String, String, String)
	 * @param ids
	 * @param table
	 * @param timestamp
	 * @return
	 */
	public DBCIterator<byte[][]> queryWithTime(List<Object[]> ids, String table, String timestamp) {
		return queryWithTime(ids, table, timestamp, activeTableSchema);
	}

	/**
	 * Returns an iterator over all rows in the table with matching id and a
	 * timestamp newer (>) than <code>timestamp</code>. The Iterator will use
	 * threads, memory and a connection until all matches are returned.
	 * 
	 * @param id
	 *            - List with primary keys
	 * @param table
	 *            - table to query
	 * @param timestamp
	 *            - timestamp (only rows with newer timestamp are returned)
	 * 
	 * @return - pmid and xml as an Iterator<byte[][]>
	 */
	public DBCIterator<byte[][]> queryWithTime(List<Object[]> ids, String table, String timestamp, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);
		String timestampWhere = fieldConfig.getTimestampFieldName() + " > " + timestamp;
		return new ArrayIter(ids, table, timestampWhere, schemaName);
	}

	/**
	 * Returns an iterator over the XML column in the default data table. The
	 * Iterator will use threads, memory and a connection until all matches are
	 * returned.
	 * 
	 * @return - results as an Iterator<byte[]>
	 */
	// TODO: remove!
	public Iterator<byte[]> queryAll() {
		return new Iter(DEFAULT_FIELD, DEFAULT_TABLE);
	}

	/**
	 * Returns an iterator over the column <code>field</code> in the table
	 * <code>table</code>. NOTE: The Iterator will use threads, memory and a
	 * connection until the iterator is empty, i.e. <code>hasNext()</code> returns
	 * null!
	 * 
	 * @param field
	 *            - field to return
	 * @param table
	 *            - table to query
	 * @return - results as an Iterator<byte[]>
	 */
	public Iterator<byte[]> queryAll(String field, String table) {
		return new Iter(field, table);
	}

	// /**
	// * Returns an iterator over all rows with matching PMID. The Iterator will
	// * use memory and block a connection until all matches where returned!
	// *
	// * @param ids
	// * - list of PMIDs
	// * @return - results as an Iterator<byte[]>
	// */
	// public Iterator<byte[]> query(List<String> ids) {
	// return new Iter(ids, DEFAULT_FIELD, DEFUALT_TABLE, DEFAULT_CONDITION);
	// }

	/**
	 * @see #query(List, String, String)
	 * @param keys
	 * @param table
	 * @return
	 */
	public Iterator<byte[]> query(List<String[]> keys, String table) {
		return new Iter(keys, DEFAULT_FIELD, table, activeTableSchema);
	}

	/**
	 * Returns an iterator over all rows with matching primary key. The key can
	 * consist of multiple parts, which are defined in the xml-configuration. The
	 * Iterator will use threads, memory and a connection until all matches were
	 * returned!
	 * 
	 * @param keys
	 *            - list of String[] containing the parts of the primary key
	 * @param table
	 *            - table to query
	 * @return - results as an Iterator<byte[]>
	 */
	public Iterator<byte[]> query(List<String[]> keys, String table, String schemaName) {
		return new Iter(keys, DEFAULT_FIELD, table, schemaName);
	}

	// /**
	// * Returns an iterator over all rows, field/table/condition can be
	// * specified. The Iterator will use memory and block a connection until
	// all
	// * matches are returned!
	// *
	// * @param ids
	// * - list of PMIDs
	// * @param field
	// * - name of the field to select
	// * @param table
	// * - name of the table to select from
	// * @param condition
	// * - name of the condition for the WHERE-clause
	// * @return - results as an Iterator<byte[]>
	// */
	// public Iterator<byte[]> query(List<String> ids, String field, String
	// table,
	// String condition) {
	// return new Iter(ids, field, table, condition);
	// }

	/**
	 * @see #queryIDAndXML(List, String, String)
	 * @param ids
	 * @param table
	 * @return
	 */
	public DBCIterator<byte[][]> queryIDAndXML(List<Object[]> ids, String table) {
		return queryIDAndXML(ids, table, activeTableSchema);
	}

	/**
	 * TODO This description is deprecated, write a new, correct version (all fields
	 * to be retrieved are returned actually).
	 * 
	 * Returns an iterator over all rows with matching pmid, containing PMID and XML
	 * as byte[]. The Iterator will use memory and block a connection until all
	 * matches are returned!
	 * 
	 * @param ids
	 *            - list of PMIDs
	 * @param table
	 *            - name of the table to select from
	 * @return - byte[][], containing pmid byte[] as [0] and xml byte[] as [1]
	 */
	// TODO the name is deprecated; in fact, all fields with "retrieve=true" are
	// returned in the byte[][]
	public DBCIterator<byte[][]> queryIDAndXML(List<Object[]> ids, String table, String schemaName) {
		return new ArrayIter(ids, table, schemaName);
	}

	// Retrieves data joined from multiple tables.
	public DBCIterator<byte[][]> queryIDAndXML(List<Object[]> ids, String[] table, String[] schemaName) {
		return new ArrayIter(ids, table, schemaName);
	}

	/**
	 * <p>
	 * Returns all column data from the data table <code>tableName</code> which is
	 * marked as 'to be retrieved' in the table scheme specified by the active table
	 * scheme.
	 * </p>
	 * <p>
	 * For more specific information, please refer to
	 * {@link #queryDataTable(String, String, String)}.
	 * </p>
	 * 
	 * @param tableName
	 *            Name of a data table.
	 * @param whereCondition
	 *            Optional additional specifications for the SQL "SELECT" statement.
	 * @param schemaName
	 *            The table schema name to determine which columns should be
	 *            retrieved. // * @return An iterator over <code>byte[][]</code> .
	 *            Each returned byte array contains one nested byte array for each
	 *            retrieved column, holding the column's data in a sequence of
	 *            bytes.
	 * @see #queryDataTable(String, String, String)
	 */
	public DBCIterator<byte[][]> queryDataTable(String tableName, String whereCondition) {
		return queryDataTable(tableName, whereCondition, activeTableSchema);
	}

	/**
	 * <p>
	 * Returns all column data from the data table <code>tableName</code> which is
	 * marked as 'to be retrieved' in the table scheme specified by
	 * <code>schemaName</code>.
	 * </p>
	 * <p>
	 * This method offers direct access to the table data by using an SQL
	 * <code>ResultSet</code> in cursor mode, allowing for queries leading to large
	 * results.
	 * </p>
	 * <p>
	 * An optional where clause (actually everything behind the "FROM" in the SQL
	 * select statement) may be passed to restrict the columns being returned. All
	 * specifications are allowed which do not alter the number of columns returned
	 * (like "GROUP BY").
	 * </p>
	 * 
	 * @param tableName
	 *            Name of a data table.
	 * @param whereCondition
	 *            Optional additional specifications for the SQL "SELECT" statement.
	 * @param schemaName
	 *            The table schema name to determine which columns should be
	 *            retrieved. // * @return An iterator over <code>byte[][]</code> .
	 *            Each returned byte array contains one nested byte array for each
	 *            retrieved column, holding the column's data in a sequence of
	 *            bytes.
	 */
	public DBCIterator<byte[][]> queryDataTable(String tableName, String whereCondition, String schemaName) {
		if (!tableExists(tableName))
			throw new IllegalArgumentException("Table \"" + tableName + "\" does not exist.");

		final FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		// Build the correct query.
		String query = null;
		String selectedColumns = StringUtils.join(fieldConfig.getColumnsToRetrieve(), ",");
		// prepend there WHERE keyword if not already present and if we don't
		// actually have only a LIMIT constraint
		if (whereCondition != null && !whereCondition.trim().toUpperCase().startsWith("WHERE")
				&& !whereCondition.trim().toUpperCase().matches("LIMIT +[0-9]+"))
			query = String.format("SELECT %s FROM %s WHERE %s", selectedColumns, tableName, whereCondition);
		else if (whereCondition != null)
			query = String.format("SELECT %s FROM %s %s", selectedColumns, tableName, whereCondition);
		else
			query = String.format("SELECT %s FROM %s", selectedColumns, tableName);
		final String finalQuery = query;

		try {

			DBCIterator<byte[][]> it = new DBCIterator<byte[][]>() {

				private Connection conn = getConn();
				private ResultSet rs = doQuery(conn);
				private boolean hasNext = rs.next();

				private ResultSet doQuery(Connection conn) throws SQLException {
					// Get a statement which is set to cursor mode. The data
					// table could
					// be really large and we don't have the two fold process
					// here where
					// first we get IDs from a subset and then only the actual
					// documents
					// for these IDs.
					conn.setAutoCommit(false);
					Statement stmt = conn.createStatement();
					stmt.setFetchSize(queryBatchSize);
					return stmt.executeQuery(finalQuery);
				}

				@Override
				public boolean hasNext() {
					if (!hasNext)
						close();
					return hasNext;
				}

				@Override
				public byte[][] next() {
					if (hasNext) {
						List<Map<String, String>> fields = fieldConfig.getFields();
						try {
							byte[][] retrievedData = new byte[fieldConfig.getColumnsToRetrieve().length][];
							for (int i = 0; i < retrievedData.length; i++) {
								retrievedData[i] = rs.getBytes(i + 1);
								if (Boolean.parseBoolean(fields.get(i).get(JulieXMLConstants.GZIP)))
									retrievedData[i] = JulieXMLTools.unGzipData(retrievedData[i]);
							}
							hasNext = rs.next();
							if (!hasNext)
								close();
							return retrievedData;
						} catch (SQLException | IOException e) {
							hasNext = false;
							e.printStackTrace();
						}
					}
					return null;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}

				@Override
				public void close() {
					try {
						if (!conn.isClosed()) {
							conn.commit();
							conn.setAutoCommit(true);
							conn.close();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			};

			return it;
		} catch (SQLException e) {
			LOG.error("Error while executing SQL statement \"" + finalQuery + "\"");
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @see #querySubset(String, String, long)
	 * @param tableName
	 * @param limitParam
	 * @return
	 * @throws SQLException
	 */
	public DBCIterator<byte[][]> querySubset(String tableName, long limitParam) throws SQLException {
		return querySubset(tableName, null, limitParam, 0, activeTableSchema);
	}

	/**
	 * <p>
	 * Retrieves XML field values in the data table referenced by the subset table
	 * <code>tableName</code> or <code>tableName</code> itself if it is a data
	 * table.
	 * </p>
	 * <p>
	 * The method always first retrieves a batch of primary keys from the subset
	 * table and then gets the actual documents from the data table (necessary for
	 * the data table - subset paradigm). As this is unnecessary when querying
	 * directly from a data table, for that kind of queries this method calls
	 * {@link #queryDataTable(String, String, String)}.
	 * </p>
	 * <p>
	 * The number of returned documents is restricted in number by
	 * <code>limitParam</code>. All documents are returned if
	 * <code>limitParam</code> is of negative value.<br>
	 * <b>Note:</b> Of course, <code>whereClause</code> could already contain an SQL
	 * 'LIMIT' specification. However, I won't work as expected since this limit
	 * expression would be applied to each batch of subset-IDs which is used to
	 * query the data table. Using the <code>limitParam</code> parameter will assure
	 * you get at most as much documents from the iterator as specified. If
	 * <code>tableName</code> denotes a data table and <code>whereClause</code> does
	 * not already contain a 'LIMIT' expression, <code>limitParam</code> will be
	 * added to <code>whereClause</code> for the subsequent call to
	 * <code>queryDataTable</code>.
	 * </p>
	 * 
	 * @param tableName
	 *            Subset table determining which documents to retrieve from the data
	 *            table; may also be a data table itself.
	 * @param whereClause
	 *            An SQL where clause restricting the returned columns of each
	 *            queried subset-ID batch. This clause must not change the rows
	 *            returned (e.g. by 'GROUP BY').
	 * @param limitParam
	 *            Number restriction of documents to return.
	 * @param numberRefHops
	 * @param schemaName
	 *            The name of table schema of the referenced data table.
	 * @return An iterator returning documents references from or in the table
	 *         <code>tableName</code>.
	 * @throws SQLException
	 * @see #queryDataTable(String, String, String)
	 */
	public DBCIterator<byte[][]> querySubset(final String tableName, final String whereClause, final long limitParam,
			Integer numberRefHops, final String schemaName) throws SQLException {
		if (!tableExists(tableName))
			throw new IllegalArgumentException("Table \"" + tableName + "\" does not exist.");

		final FieldConfig fieldConfig = fieldConfigs.get(schemaName);
		final String dataTable = getReferencedTable(tableName, numberRefHops);
		if (dataTable.equals(tableName)) {
			String newWhereClause = whereClause;
			if (newWhereClause == null && limitParam > 0)
				newWhereClause = "";
			// For the current method, limit must be given explicitly. Not so
			// for querying a single table like the data table. If the
			// whereClause not already contains a LIMIT expression, we just add
			// it corresponding to the limit parameter.
			if (limitParam > 0 && !newWhereClause.toLowerCase().matches(".*limit +[0-9]+.*"))
				newWhereClause += " LIMIT " + limitParam;
			return queryDataTable(tableName, newWhereClause, schemaName);
		}

		final Connection conn = getConn();
		try {
			// We will set the key-retrieval-statement below to cursor mode by
			// specifying a maximum number of rows to return; for this to work,
			// auto commit must be turned off.
			conn.setAutoCommit(false);
			final Statement stmt = conn.createStatement();
			// Go to cursor mode by setting a fetch size.
			stmt.setFetchSize(queryBatchSize);
			// As we want to query the whole subset/data table, just get a
			// cursor over all IDs in the set.
			final ResultSet outerKeyRS = stmt
					.executeQuery("SELECT (" + fieldConfig.getPrimaryKeyString() + ") FROM " + tableName);

			DBCIterator<byte[][]> it = new DBCIterator<byte[][]>() {

				private long returnedDocs = 0;
				private ResultSet keyRS = outerKeyRS;
				private long limit = limitParam < 0 ? Long.MAX_VALUE : limitParam;
				private Iterator<byte[][]> xmlIt;

				@Override
				public boolean hasNext() {
					if (returnedDocs >= limit)
						return false;

					try {
						if (xmlIt == null || !xmlIt.hasNext()) {
							int currentBatchSize = 0;
							List<Object[]> ids = new ArrayList<Object[]>();
							String[] pks = fieldConfig.getPrimaryKey();
							while (currentBatchSize < queryBatchSize && keyRS.next()) {
								String[] values = new String[pks.length];
								for (int i = 0; i < pks.length; i++) {
									values[i] = (String) keyRS.getObject(i + 1);
								}
								ids.add(values);
								++currentBatchSize;
							}
							if (whereClause != null)
								xmlIt = new ArrayIter(ids, dataTable, whereClause, schemaName);
							else
								xmlIt = new ArrayIter(ids, dataTable, schemaName);

							boolean xmlItHasNext = xmlIt.hasNext();
							if (!xmlItHasNext)
								close();

							return xmlItHasNext;
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
					return true;
				}

				@Override
				public byte[][] next() {
					if (!hasNext()) {
						close();
						return null;
					}
					++returnedDocs;
					return xmlIt.next();
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}

				@Override
				public void close() {
					((ArrayIter) xmlIt).close();
					try {
						if (!conn.isClosed()) {
							conn.close();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

			};

			return it;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * An iterator that returns documents stored in the database identified by the
	 * primary keys delivered in a list upon creation of the iterator. The returned
	 * data corresponds to the table schema also given at iterator creation.<br>
	 * The iterator employs two threads to retrieve new documents from the database
	 * while other documents, fetched before, can be processed concurrently. The
	 * idea is that the database can work in parallel to the program working with
	 * the retrieved documents.
	 * 
	 * 
	 * @author hellrich
	 * 
	 */
	private class ArrayIter extends DBCThreadedIterator<byte[][]> {
		// To query by PMID, uses two other threads
		public ArrayIter(List<Object[]> ids, String table, String schemaName) {
			String[] tables = new String[1];
			String[] schemaNames = new String[1];
			tables[0] = table;
			schemaNames[0] = schemaName;
			backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, null, schemaNames);
			update();
		}

		// To query by PMID, uses two other threads
		public ArrayIter(List<Object[]> ids, String table, String whereClause, String schemaName) {
			String[] tables = new String[1];
			String[] schemaNames = new String[1];
			tables[0] = table;
			schemaNames[0] = schemaName;
			backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, whereClause, schemaNames);
			update();
		}

		// Used when multiple tables are joined.
		public ArrayIter(List<Object[]> ids, String[] tables, String[] schemaNames) {
			backgroundThread = new ArrayResToListThread(listExchanger, ids, tables, null, schemaNames);
			update();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see de.julielab.xmlData.dataBase.DBCThreadedIterator#destroy()
		 */
		@Override
		public void close() {
			super.close();
			((ArrayResToListThread) backgroundThread).end();
		}

	}

	/*******************************
	 * Classes for queryIDAndXML
	 *******************************************/
	/**
	 * This class converts a <tt>ResultSet</tt>, retrieved from the database, into a
	 * list, that is then returned.<br>
	 * This class is a <tt>Thread</tt> and serves as an intermediate layer between
	 * the program that uses the resulting list and another thread that is doing the
	 * actual database querying. This class has to {@link Exchanger}: One
	 * <tt>Exchanger</tt> for the result list that is returned to the caller.
	 * Another <tt>Exchanger</tt> is used to retrieve database results in the form
	 * of {@link ResultSet} instances from the thread querying the database. In
	 * between, this class converts the <tt>ResultSet</tt> to a <tt>List</tt>.
	 * 
	 * @author hellrich
	 * 
	 */
	private class ArrayResToListThread extends Thread {
		private Exchanger<List<byte[][]>> listExchanger;
		private Exchanger<ResultSet> resExchanger = new Exchanger<ResultSet>();
		private ResultSet currentRes;
		private ArrayList<byte[][]> currentList;
		private String[] table;
		private String[] schemaName;
		private boolean joined = false;

		private volatile boolean end = false;
		private final ArrayFromDBThread arrayFromDBThread;

		ArrayResToListThread(Exchanger<List<byte[][]>> listExchanger, List<Object[]> keyList, String[] table,
				String whereClause, String[] schemaName) {
			this.listExchanger = listExchanger;
			this.table = table;
			this.schemaName = schemaName;
			if (table.length > 1 && schemaName.length > 1) {
				this.joined = true;
			}
			// start the thread that is actually querying the database
			arrayFromDBThread = new ArrayFromDBThread(resExchanger, keyList, table, whereClause, schemaName);
			try {
				// retrieve the first result without yet running the thread;
				// when we have the result, we begin to create the result list
				// out of the first retrieved ResultSet and return the list,
				// then get the next results and so on...
				currentRes = resExchanger.exchange(null);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			setDaemon(true);
			start();
		}

		@SuppressWarnings("unchecked")
		public void run() {
			List<Object> numColumnsAndFields = getNumColumnsAndFields(joined, table, schemaName);
			int numColumns = (Integer) numColumnsAndFields.get(0);
			List<Map<String, String>> fields = (List<Map<String, String>>) numColumnsAndFields.get(1);
			int i = 0;
			byte[][] retrievedData = null;
			try {
				while (currentRes != null && !end) {
					currentList = new ArrayList<byte[][]>();
					// convert the current database ResultSet into a list.
					while (currentRes.next()) {
						retrievedData = new byte[numColumns][];
						for (i = 0; i < retrievedData.length; i++) {
							retrievedData[i] = currentRes.getBytes(i + 1);
							if (Boolean.parseBoolean(fields.get(i).get(JulieXMLConstants.GZIP))
									&& retrievedData[i] != null)
								retrievedData[i] = JulieXMLTools.unGzipData(retrievedData[i]);
						}
						currentList.add(retrievedData);
					} // end ResultSet to List conversion
						// Offer the created result list to the calling program;
						// as soon as the result has been given away, we
						// continue fetching more documents from the database
						// below, allowing the calling program to process the
						// current result and already fetching the next
					if (!currentList.isEmpty())
						listExchanger.exchange(currentList);
					// Get the next ResultSet from the database
					currentRes = resExchanger.exchange(null);
				}
				listExchanger.exchange(null); // stop signal
			} catch (InterruptedException | SQLException | IOException e) {
				LOG.error(
						"Exception occured while reading " + "data from result set, index {}. "
								+ "Corresponding field in schema definition is: {}. Read data was: \"{}\"",
						new Object[] { i + 1, fields.get(i), new String(retrievedData[i]) });
				e.printStackTrace();
			} catch (NullPointerException e) {
				LOG.debug("NPE on: Index {}, field {}, data {}",
						new Object[] { i, fields.get(i), retrievedData != null ? retrievedData[i] : null });
				throw e;
			}
		}

		/**
		 * Must be called when the thread is no longer required. Otherwise, it will
		 * continue querying the database.
		 */
		public void end() {
			arrayFromDBThread.end();
			end = true;
		}
	}

	/**
	 * This class is last <tt>Thread</tt> in the
	 * <code>Iterator - ResultSet to List converter - ResultSet from database retriever</code>
	 * chain and thus is the class doing the database querying. <br>
	 * Upon creation, this class starts itself as a demon <tt>Thread</tt>. It
	 * queries {@link DataBaseConnector#queryBatchSize} IDs and offers the
	 * <tt>ResultSet</tt> in an {@link Exchanger} for the intermediate
	 * <tt>Thread</tt>.
	 * 
	 * @author hellrich
	 * 
	 */
	private class ArrayFromDBThread extends Thread {
		private Iterator<Object[]> keyIter;
		private Exchanger<ResultSet> resExchanger;
		private StringBuilder queryBuilder;
		private ResultSet currentRes;
		private String selectFrom;
		private Connection conn;
		private String whereClause = null;
		private FieldConfig fieldConfig;
		private volatile boolean end;
		private boolean joined = false;
		private String dataTable;
		private String dataSchema;

		public ArrayFromDBThread(Exchanger<ResultSet> resExchanger, List<Object[]> keyList, String[] table,
				String whereClause, String[] schemaName) {
			this.conn = getConn();
			this.resExchanger = resExchanger;
			keyIter = keyList.iterator();
			this.queryBuilder = new StringBuilder();
			this.whereClause = whereClause;
			this.dataTable = table[0];
			this.dataSchema = schemaName[0];
			if (table.length > 1 && schemaName.length > 1) {
				this.joined = true;
			}
			buildSelectFrom(table, schemaName);
			setDaemon(true);
			start();
		}

		/**
		 * Create the basic SQL query structure used to query documents from the
		 * database.
		 * 
		 * @param table
		 * @param schemaName
		 */
		private void buildSelectFrom(String[] table, String[] schemaName) {
			// Build SELECT if there is only one table.
			if (!joined) {
				fieldConfig = fieldConfigs.get(dataSchema);
				selectFrom = "SELECT " + StringUtils.join(fieldConfig.getColumnsToRetrieve(), ",") + " FROM "
						+ dataTable + " WHERE ";
				// Build SELECT if multiple tables will be joined.
				// This will be in the form
				// 'SELECT dataTable.pmid, otherTable1.data, otherTable2.data
				// FROM dataTable
				// LEFT JOIN otherTable1 ON dataTable.pmid=otherTable1.pmid
				// LEFT JOIN otherTable2 ON dataTable.pmid=othertable2.pmid
				// WHERE (dataTable.pmid=1) OR (dataTable.pmid=2) OR ...'
			} else {
				String[] primaryKey = null;
				ArrayList<String> select = new ArrayList<String>();
				ArrayList<String> leftJoin = new ArrayList<String>();

				for (int i = 0; i < table.length; i++) {
					fieldConfig = fieldConfigs.get(schemaName[i]);
					String[] columnsToRetrieve = fieldConfig.getColumnsToRetrieve();
					for (int j = 0; j < columnsToRetrieve.length; j++) {
						String column = table[i] + "." + columnsToRetrieve[j];
						select.add(column);
					}
					if (i == 0) {
						// Get the names of the primary keys once, since they
						// should be identical for all tables.
						primaryKey = fieldConfig.getPrimaryKey();
					} else {
						String primaryKeyMatch = "";
						for (int j = 0; j < primaryKey.length; j++) {
							primaryKeyMatch = table[0] + "." + primaryKey[j] + "=" + table[i] + "." + primaryKey[j];
							if (!(j == primaryKey.length - 1))
								primaryKeyMatch = primaryKeyMatch + " AND ";
						}
						String join = "LEFT JOIN " + table[i] + " ON " + primaryKeyMatch;
						leftJoin.add(join);
					}
				}
				selectFrom = "SELECT " + StringUtils.join(select, ",") + " FROM " + table[0] + " "
						+ StringUtils.join(leftJoin, " ") + " WHERE ";
				LOG.trace("Querying data via SQL: {}", selectFrom);
			}
		}

		/**
		 * Fetches results as long as there are unprocessed documents in the given
		 * subset table.
		 */
		public void run() {
			try {
				while (keyIter.hasNext() && !end) {
					currentRes = getFromDB();
					resExchanger.exchange(currentRes);
				}
				resExchanger.exchange(null); // Indicates end
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * Builds the final SQL query specifying the exact primary keys for retrieval,
		 * performs the actual query and returns the respective <tt>ResultSet</tt>
		 * 
		 * @return
		 */
		private ResultSet getFromDB() {
			ResultSet res = null;
			String sql = null;
			try {
				queryBuilder.delete(0, queryBuilder.capacity());
				Statement stmt = conn.createStatement();
				List<String> pkConjunction = new ArrayList<String>();
				for (int i = 0; keyIter.hasNext() && i < queryBatchSize; ++i) {
					// get the next row of primary key values, e.g.
					//
					// pmid | systemID
					// --------------------
					// 1564 | FSU <--- this is stored in "keys"
					Object[] keys = keyIter.next();
					String[] nameValuePairs;
					nameValuePairs = new String[keys.length];
					// build an array of pairs like
					// ["pmid = 1563", "systemID = FSU"]
					if (!joined) {
						for (int j = 0; j < keys.length; ++j) {
							String fieldName = fieldConfig.getPrimaryKey()[j];
							nameValuePairs[j] = String.format("%s = '%s'", fieldName, keys[j]);
						}
					} else {
						for (int j = 0; j < keys.length; ++j) {
							String fieldName = fieldConfig.getPrimaryKey()[j];
							nameValuePairs[j] = String.format("%s = '%s'", dataTable + "." + fieldName, keys[j]);
						}
					}
					// make a conjunction of the name value pairs:
					// "(pmid = 1563 AND systemID = FSU)"
					pkConjunction.add("(" + StringUtils.join(nameValuePairs, " AND ") + ")");
				}
				queryBuilder.append(selectFrom);
				queryBuilder.append("(");
				queryBuilder.append(StringUtils.join(pkConjunction, " OR "));
				queryBuilder.append(")");
				if (whereClause != null)
					queryBuilder.append(" AND " + whereClause);
				sql = queryBuilder.toString();
				LOG.trace("Fetching data with command \"{}\"", sql);
				res = stmt.executeQuery(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				LOG.error("SQL: " + sql);
			}
			return res;
		}

		public void end() {
			// The connection is closed automatically when the thread ends, see
			// the run() method.
			end = true;
		}

	}

	/**
	 * The Iterator for byte[]
	 * 
	 * @author hellrich
	 * 
	 */
	private class Iter extends DBCThreadedIterator<byte[]> {
		// To query by PMID, uses two other threads
		public Iter(List<String[]> keys, String field, String table, String schemaName) {
			backgroundThread = new ResToListThread(listExchanger, keys, field, table, schemaName);
			update();
		}

		// To query all, uses only one other thread
		public Iter(String field, String table) {
			backgroundThread = new ListFromDBThread(listExchanger, field, table);
			update();
		}
	}

	/*******************************
	 * Classes for query()
	 *******************************************/
	/**
	 * A second thread, making a list out of ResultSets
	 * 
	 * @author hellrich
	 * 
	 */
	private class ResToListThread extends Thread {
		private Exchanger<List<byte[]>> listExchanger;
		private Exchanger<ResultSet> resExchanger = new Exchanger<ResultSet>();
		private ResultSet currentRes;
		private ArrayList<byte[]> currentList;
		private final String field;

		ResToListThread(Exchanger<List<byte[]>> listExchanger, List<String[]> keys, String field, String table,
				String schemaName) {
			this.listExchanger = listExchanger;
			this.field = field;
			new FromDBThread(resExchanger, keys, field, table, schemaName);
			try {
				currentRes = resExchanger.exchange(null);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			setDaemon(true);
			start();
		}

		public void run() {
			boolean doUnGzip = Boolean
					.parseBoolean(fieldConfigs.get(activeTableSchema).getField(field).get(JulieXMLConstants.GZIP));
			try {
				while (currentRes != null) {
					currentList = new ArrayList<byte[]>();
					while (currentRes.next()) {
						byte[] bytes = currentRes.getBytes(1);
						if (doUnGzip)
							bytes = JulieXMLTools.unGzipData(bytes);
						currentList.add(bytes);
					}
					if (!currentList.isEmpty())
						listExchanger.exchange(currentList);
					currentRes = resExchanger.exchange(null);
				}
				listExchanger.exchange(null); // stop signal
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * A third thread, querying the database for ResultSets
	 * 
	 * @author hellrich
	 * 
	 */
	private class FromDBThread extends Thread {
		private Iterator<String[]> keyIter;
		private Exchanger<ResultSet> resExchanger;
		private String statement;
		private ResultSet currentRes;
		private Connection conn;
		private FieldConfig fieldConfig;

		public FromDBThread(Exchanger<ResultSet> resExchanger, List<String[]> keys, String field, String table,
				String schemaName) {
			this.conn = getConn();
			this.resExchanger = resExchanger;
			this.fieldConfig = fieldConfigs.get(schemaName);
			statement = "SELECT " + field + " FROM " + table + " WHERE ";
			keyIter = keys.iterator();
			setDaemon(true);
			start();
		}

		public void run() {
			try {
				while (keyIter.hasNext()) {
					currentRes = getFromDB();
					resExchanger.exchange(currentRes);
				}
				resExchanger.exchange(null); // Indicates end
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		private ResultSet getFromDB() {
			ResultSet res = null;
			StringBuilder sql = new StringBuilder(statement);
			Object[] values = null;
			try {
				Statement st = conn.createStatement();
				String[] pks = fieldConfig.getPrimaryKey();

				int i = 0;
				while (keyIter.hasNext() && i < queryBatchSize) {
					values = keyIter.next();
					for (int j = 0; j < pks.length; ++j) {
						if (fieldConfig.isOfStringType(pks[j]))
							sql.append(pks[j]).append("=\'").append(values[j]).append("\'");
						else
							sql.append(pks[j]).append("=").append(values[j]);
						if (j < pks.length - 1)
							sql.append(" AND ");
					}
					sql.append(" OR ");
					++i;
				}
				sql.delete(sql.length() - 4, sql.length()); // Remove trailing
															// " OR "
				res = st.executeQuery(sql.toString());
			} catch (SQLException e) {
				e.printStackTrace();
				System.err.println(sql.toString());
			} catch (ArrayIndexOutOfBoundsException e) {
				LOG.error("Configuration file and query are incompatible.");
				String wrongLine = "";
				if (values != null)
					for (int i = 0; i < values.length; ++i)
						wrongLine += values[i];
				LOG.error("Error in line beginning with: " + wrongLine);
			}
			return res;
		}

	}

	public List<Object> getNumColumnsAndFields(boolean joined, String[] tables, String[] schemaNames) {
		List<Object> numColumnsAndFields = new ArrayList<Object>();
		int numColumns = 0;
		List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
		if (!joined) {
			FieldConfig fieldConfig = fieldConfigs.get(schemaNames[0]);
			numColumns = fieldConfig.getColumnsToRetrieve().length;
			fields = fieldConfig.getFields();
		} else {
			for (int i = 0; i < tables.length; i++) {
				FieldConfig fieldConfig = fieldConfigs.get(schemaNames[i]);
				int num = fieldConfig.getColumnsToRetrieve().length;
				numColumns = numColumns + num;
				List<Map<String, String>> fieldsPartly = fieldConfig.getFieldsToRetrieve();
				fields.addAll(fieldsPartly);
			}
		}
		numColumnsAndFields.add(numColumns);
		numColumnsAndFields.add(fields);
		return numColumnsAndFields;
	}

	/**************************
	 * Class for queryAll()
	 *****************************************/
	/**
	 * Variant, a thread querying the whole database for ResultSets, returning all
	 * entries (as lists) to the iterator (replaces threads 2 and 3)
	 * 
	 * @author hellrich
	 * 
	 */
	private class ListFromDBThread extends Thread {
		private Exchanger<List<byte[]>> listExchanger;
		private ArrayList<byte[]> currentList;
		private String selectFrom;
		private boolean hasNext;
		private ResultSet res;
		private Connection conn;
		private boolean doUnGzip;

		public ListFromDBThread(Exchanger<List<byte[]>> listExchanger, String field, String table) {
			this.listExchanger = listExchanger;
			selectFrom = String.format("SELECT %s FROM %s", field, table);
			doUnGzip = Boolean
					.parseBoolean(fieldConfigs.get(activeTableSchema).getField(field).get(JulieXMLConstants.GZIP));
			try {
				conn = getConn();
				conn.setAutoCommit(false);// cursor doesn't work otherwise
				Statement st = conn.createStatement();
				st.setFetchSize(queryBatchSize); // cursor
				res = st.executeQuery(selectFrom);
				hasNext = res.next(); // never forget
				updateCurrentList();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			setDaemon(true);
			start();
		}

		public void run() {
			try {
				listExchanger.exchange(currentList);
				while (hasNext) {
					updateCurrentList();
					listExchanger.exchange(currentList);
				}
				conn.setAutoCommit(true);
				listExchanger.exchange(new ArrayList<byte[]>()); // Empty
																	// lists
																	// as
																	// stop
																	// signal
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		private void updateCurrentList() {
			currentList = new ArrayList<byte[]>();
			int i = 0;
			try {
				while (hasNext && i < queryBatchSize) {
					byte[] bytes = res.getBytes(1);
					if (doUnGzip)
						bytes = JulieXMLTools.unGzipData(bytes);
					currentList.add(bytes);
					++i;
					hasNext = res.next();
				}
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Returns a map with information about how many rows are marked as
	 * <tt>is_in_process</tt>, <tt>is_processed</tt> and how many rows there are in
	 * total.<br/>
	 * The respective values are stored under with the keys
	 * {@link Constants#IN_PROCESS}, {@link Constants#PROCESSED} and
	 * {@link Constants#TOTAL}.
	 * 
	 * @param subsetTableName
	 *            name of the subset table to gain status information for
	 * @return A SubsetStatus instance containing status information about the
	 *         subset table <tt>subsetTableName</tt>
	 * @throws TableNotFoundException
	 *             If <tt>subsetTableName</tt> does not point to a database table.
	 */
	public SubsetStatus status(String subsetTableName) throws TableNotFoundException {
		if (!tableExists(subsetTableName))
			throw new TableNotFoundException("The subset table \"" + subsetTableName + "\" does not exist.");

		SubsetStatus status = new SubsetStatus();

		Connection conn = null;
		try {
			conn = getConn();
			String sql = String.format(
					"SELECT sum(case when %s=TRUE then 1 end) as %s, " + "sum(case when %s=TRUE then 1 end) as %s,"
							+ "sum(case when %s=TRUE then 1 end) as %s," + "sum(1) as %s FROM %s",
					Constants.HAS_ERRORS, Constants.HAS_ERRORS, Constants.IS_PROCESSED, Constants.IS_PROCESSED,
					Constants.IN_PROCESS, Constants.IN_PROCESS, Constants.TOTAL, subsetTableName);
			Statement stmt = conn.createStatement();
			{
				ResultSet res = stmt.executeQuery(sql);
				if (res.next()) {
					status.hasErrors = res.getLong(Constants.HAS_ERRORS);
					status.inProcess = res.getLong(Constants.IN_PROCESS);
					status.isProcessed = res.getLong(Constants.IS_PROCESSED);
					status.total = res.getLong(Constants.TOTAL);
				}
			}

			{
				SortedMap<String, Long> pipelineStates = new TreeMap<>();
				status.pipelineStates = pipelineStates;
				String pipelineStateSql = String.format("SELECT %s,count(%s) from %s group by %s",
						Constants.LAST_COMPONENT, Constants.LAST_COMPONENT, subsetTableName, Constants.LAST_COMPONENT);
				ResultSet res = stmt.executeQuery(pipelineStateSql);
				while (res.next())
					pipelineStates.put(res.getString(1) != null ? res.getString(1) : "<empty>", res.getLong(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}

		return status;
	}

	/**
	 * @return - all tables in the active scheme
	 */
	public ArrayList<String> getTables() {
		Connection conn = getConn();
		ArrayList<String> tables = new ArrayList<String>();
		try {
			ResultSet res = conn.getMetaData().getTables(null, dbConfig.getActivePGSchema(), null,
					new String[] { "TABLE" });
			while (res.next())
				tables.add(res.getString("TABLE_NAME"));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return tables;
	}

	/**
	 * Query the MetaData for the columns of a table
	 * 
	 * @param tableName
	 *            - the table
	 * @return - List of String containing name and type of each column
	 */
	public List<String> getTableDefinition(String tableName) {
		Connection conn = getConn();
		ArrayList<String> columns = new ArrayList<String>();
		String schema;
		if (tableName.contains(".")) {
			schema = tableName.split("\\.")[0];
			tableName = tableName.split("\\.")[1];
		} else
			schema = dbConfig.getActivePGSchema();
		try {
			ResultSet res = conn.getMetaData().getColumns(null, schema, tableName, null);
			// ERIK 6th of December 2013: Removed the type information because
			// it lead to false positives: When the
			// dbcConfiguration specifies an "integer", it actually becomes an
			// "int4". This could be treated, for the
			// moment
			// only the names will be checked.
			while (res.next())
				// columns.add(res.getString("COLUMN_NAME") + " " +
				// res.getString("TYPE_NAME"));
				columns.add(res.getString("COLUMN_NAME"));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return columns;
	}

	/**
	 * @return - the active Postgres scheme
	 */
	public String getScheme() {
		String scheme = "none";
		try {
			ResultSet res = getConn().createStatement().executeQuery("SHOW search_path;");
			if (res.next())
				scheme = res.getString(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return scheme;
	}

	/**
	 * 
	 * @return the active field configuration
	 */
	public FieldConfig getFieldConfiguration() {
		return fieldConfigs.get(activeTableSchema);
	}

	/**
	 * 
	 * @param schemaName
	 *            The name of the schema for which the eventual
	 *            <code>FieldConfig</code> should be returned.
	 * @return The field configuration for <code>schemaName</code>.
	 */
	public FieldConfig getFieldConfiguration(String schemaName) {
		return fieldConfigs.get(schemaName);
	}

	/**
	 * @see #checkTableDefinition(String, String)
	 * @param tableName
	 * @return
	 */
	public void checkTableDefinition(String tableName) {
		checkTableDefinition(tableName, activeTableSchema);
	}

	/**
	 * Compares the actual table in the database with its definition in the xml
	 * configuration</br>
	 * Note: This method currently does not check other then primary key columns for
	 * tables that reference another table, even if those should actually be data
	 * tables.
	 * 
	 * @param tableName
	 *            - table to check
	 */
	public void checkTableDefinition(String tableName, String schemaName) {
		FieldConfig fieldConfig = fieldConfigs.get(schemaName);

		List<String> actualColumns = new ArrayList<>();
		List<String> definedColumns = new ArrayList<>();

		// Postgres will convert table names to lower case but check for capital
		// letter names all the same, thus never
		// finding a match when giving names with capital letters.
		tableName = tableName.toLowerCase();

		// ERIK 6th of December 2013: Removed the type information because it
		// lead to false positives: When the
		// dbcConfiguration specifies an "integer", it actually becomes an
		// "int4". This could be treated, for the moment
		// only the names will be checked.

		if (getReferencedTable(tableName) == null) { // dataTable, check all
														// columns
			actualColumns = new ArrayList<>(getTableDefinition(tableName));
			for (Map<String, String> m : fieldConfig.getFields())
				// definedColumns.add(m.get("name") + " " + m.get("type"));
				definedColumns.add(m.get("name"));

		} else { // normal table, check only pk-columns
			for (Map<String, String> m : fieldConfig.getFields())
				if (new Boolean(m.get("primaryKey")))
					// definedColumns.add(m.get("name") + " " + m.get("type"));
					definedColumns.add(m.get("name"));

			// getting pk-names and types
			String schema;
			if (tableName.contains(".")) {
				schema = tableName.split("\\.")[0];
				tableName = tableName.split("\\.")[1];
			} else
				schema = dbConfig.getActivePGSchema();

			HashSet<String> pkNames = new HashSet<String>();
			Connection conn = getConn();
			try {
				ResultSet res = conn.getMetaData().getImportedKeys("", schema, tableName);
				while (res.next())
					pkNames.add(res.getString("FKCOLUMN_NAME"));
				res = conn.getMetaData().getColumns(null, schema, tableName, null);
				while (res.next()) {
					if (pkNames.contains(res.getString("COLUMN_NAME")))
						// actualColumns.add(res.getString("COLUMN_NAME") + " "
						// + res.getString("TYPE_NAME"));
						actualColumns.add(res.getString("COLUMN_NAME"));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		Collections.sort(definedColumns);
		Collections.sort(actualColumns);
		if (!definedColumns.equals(actualColumns))
			throw new IllegalStateException("Table used table schema definition \"" + schemaName
					+ "\" does not match the actual schema of the table \"" + tableName + "\": Expected: "
					+ StringUtils.join(definedColumns, " ") + "; actual: " + StringUtils.join(actualColumns, " "));

	}

	/**
	 * <p>
	 * Sets the values of <code>is_processed</code> to <code>TRUE</code> and of
	 * <code>is_in_process</code> to <code>FALSE</code> for a collection of
	 * documents according to the given primary keys.
	 * </p>
	 * 
	 * @param subsetTableName
	 *            name of the subset
	 * @param primaryKeyList
	 *            the list of primary keys which itself can consist of several
	 *            primary key elements
	 */
	public void setProcessed(String subsetTableName, ArrayList<byte[][]> primaryKeyList) {

		Connection conn = getConn();
		FieldConfig fieldConfig = fieldConfigs.get(activeTableSchema);

		String whereArgument = StringUtils.join(fieldConfig.expandPKNames("%s = ?"), " AND ");
		String update = "UPDATE " + subsetTableName + " SET is_processed = TRUE, is_in_process = FALSE" + " WHERE "
				+ whereArgument;

		try {
			conn.setAutoCommit(false);

			PreparedStatement processed = conn.prepareStatement(update);
			for (byte[][] primaryKey : primaryKeyList) {
				for (int i = 0; i < primaryKey.length; i++) {
					processed.setString(i + 1, new String(primaryKey[i]));
				}
				processed.addBatch();
			}
			processed.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * <p>
	 * Sets the value of <code>has_errors</code> to <code>TRUE</code> and adds a
	 * description in <code>log</code> for exceptions which occured during the
	 * processing of a collection of documents according to the given primary keys.
	 * </p>
	 * 
	 * @param subsetTableName
	 *            name of the subset
	 * @param primaryKeyList
	 *            the list of primary keys which itself can consist of several
	 *            primary key elements
	 * @param logException
	 *            matches primary keys of unsuccessfully processed documents and
	 *            exceptions that occured during the processing
	 */
	public void setException(String subsetTableName, ArrayList<byte[][]> primaryKeyList,
			HashMap<byte[][], String> logException) {

		Connection conn = getConn();
		FieldConfig fieldConfig = fieldConfigs.get(activeTableSchema);

		String whereArgument = StringUtils.join(fieldConfig.expandPKNames("%s = ?"), " AND ");
		String update = "UPDATE " + subsetTableName + " SET has_errors = TRUE, log = ?" + " WHERE " + whereArgument;

		try {
			conn.setAutoCommit(false);

			PreparedStatement processed = conn.prepareStatement(update);
			for (byte[][] primaryKey : primaryKeyList) {
				for (int i = 0; i < primaryKey.length; i++) {
					processed.setString(1, logException.get(primaryKey));
					processed.setString(i + 2, new String(primaryKey[i]));
				}
				processed.addBatch();
			}
			processed.executeBatch();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Returns the indices of the primary keys, beginning with 0.
	 */
	public List<Integer> getPrimaryKeyIndices() {
		FieldConfig fieldConfig = fieldConfigs.get(activeTableSchema);
		List<Integer> pkIndices = fieldConfig.getPrimaryKeyFieldNumbers();
		return pkIndices;
	}

	public void setActivePGSchema(String pgSchema) {
		dbConfig.setActivePGSchema(pgSchema);
	}

	public void checkTableSchemaCompatibility(String... schemaNames) {
		if (null == schemaNames || schemaNames.length == 0) {
			LOG.warn("No table schema names were passed - nothing to check.");
			return;
		}
		List<String> referenceKey = null;
		String referenceSchemaName = null;
		List<String> notMatchingSchemaNames = new ArrayList<>();
		for (String schemaName : schemaNames) {
			FieldConfig fieldConfig = fieldConfigs.get(schemaName);
			String[] primaryKey = fieldConfig.getPrimaryKey();
			List<String> asList = Arrays.asList(primaryKey);
			Collections.sort(asList);
			if (null == referenceKey) {
				referenceKey = asList;
				referenceSchemaName = schemaName;
			} else {
				if (!referenceKey.equals(asList))
					notMatchingSchemaNames.add(schemaName);
			}
		}
		if (!notMatchingSchemaNames.isEmpty())
			throw new IllegalArgumentException(
					"There were at least one table schema pair that is not compatible to each other because their primary keys differ. The table schema \""
							+ referenceSchemaName + "\" has another primary key than the table schema(s) \""
							+ StringUtils.join(notMatchingSchemaNames, ", ") + "\".");
	}

	public void setActiveTableSchema(String schemaName) {
		this.activeTableSchema = schemaName;
	}

	public String getDbURL() {
		return dbURL;
	}

	public void close() {
		if (dataSource instanceof HikariDataSource)
			((HikariDataSource) dataSource).close();
	}

	public boolean isDatabaseReachable() {
		Connection conn = null;
		try {
			conn = getConn();
			return true;
		} catch (Exception e) {
			LOG.warn("Got error when trying to connect to {}: {}", getDbURL(), e.getMessage());
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					LOG.warn("Couldn't close connection: ", e);
				}
		}
		return false;
	}
}
