/**
 * Constants.java
 *
 * Copyright (c) 2010, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 19.11.2010
 **/

package de.julielab.xmlData;

/**
 * This class provides Constants useful for common tasks. Examples include
 * database field names for the import or retrieval of Medline documents, table
 * names etc.
 * 
 * @author faessler
 */
public final class Constants {

	// Field attribute names

	/**
	 * The default PostgreSQL schema in which all data related tables are
	 * stored. The schema is {@value #DEFAULT_DATA_SCHEMA}.
	 */
	public static final String DEFAULT_DATA_SCHEMA = "_data";

	/**
	 * Constant for the name of a database table holding at least document ID
	 * and document data (e.g. PubmedId and Medline XML). Value:
	 * {@value #DEFAULT_DATA_TABLE_NAME}.
	 */
	public static final String DEFAULT_DATA_TABLE_NAME = DEFAULT_DATA_SCHEMA
			+ "._data";

	public static final String VALUE = "value";

	// SQL type constants

	public static final String TYPE_TEXT = "text";
	
	public static final String TYPE_TEXT_ARRAY = "text[]";

	public static final String TYPE_VARCHAR_ARRAY = "varchar[]";

	public static final String TYPE_BINARY_DATA = "bytea";

	/**
	 * Constant for a possible value of <code>type.</code>
	 * <p>
	 * Used to to create a timestamp without time zone.
	 */
	public static final String TYPE_TIMESTAMP_WITHOUT_TIMEZONE = "timestamp without time zone";

	public static final String TYPE_INTEGER = "integer";

	public static final String TYPE_BOOLEAN = "boolean";

    public static final String TYPE_XML = "xml";

	public static final String XML_FIELD_NAME = "xml";

	public static final String PMID_FIELD_NAME = "pmid";

	public static final String DATE_FIELD_NAME = "date";

	public static final String NLM_ID_FIELD_NAME = "nlm_id";

	public static final String AUTO_ID_FIELD_NAME = "autoID";

	public static final String HAS_ERRORS = "has_errors";

	public static final String LOG = "log";

	public static final String IN_PROCESS = "is_in_process";

	public static final String IS_PROCESSED = "is_processed";

	public static final String LAST_COMPONENT = "last_component";

	public static final String HOST_NAME = "host_name";

	public static final String PROCESSING_TIMESTAMP = "processing_timestamp";

	public static final String PID = "pid";

	@Deprecated
	public static final String DOC_ID_FIELD_NAME = "doc_id";

	public static final String PROCESSED = "is_processed";

	public static final String HIDDEN_CONFIG_PATH = "dbcTest.hiddenConfigPath";

	public static final String COSTOSYS_CONFIG_FILE = "costosys.configurationfile";

	public static final String MIRROR_COLLECTION_NAME = "_mirrorSubsets";

	public static final String MIRROR_COLUMN_DATA_TABLE_NAME = "datatablename";

	public static final String MIRROR_COLUMN_SUBSET_NAME = "subsettablename";

	public static final String MIRROR_COLUMN_DO_RESET = "performreset";

	public static final String TIMESTAMP_FIELD_NAME = "timestamp";

	public static final String TOTAL = "total";


}
