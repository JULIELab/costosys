/**
 * QueryCLI.java
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
 * Creation date: 20.11.2010
 **/

package de.julielab.xmlData.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import de.julielab.xmlData.Constants;
import de.julielab.xmlData.config.TableSchemaDoesNotExistException;
import de.julielab.xmlData.dataBase.DataBaseConnector;
import de.julielab.xmlData.dataBase.SubsetStatus;

/**
 * Command line interface for interaction with a databases holding e.g. Medline
 * XML data.
 * 
 * @author faessler / hellrich
 */
public class CLI {

	private final static String DELIMITER = "\n--------------------------------------------------------------------------------\n";

	private static final Logger LOG = LoggerFactory.getLogger(CLI.class);

	private static boolean verbose = false;

	public static String[] USER_SCHEME_DEFINITION = new String[]{"dbcconfiguration.xml", "costosys.xml", "costosysconfiguration.xml"};

	private static final String KEY_PART_SEPERATOR = "\t";

	private static final String FILE_SEPERATOR = System.getProperty("file.separator");

	private enum Mode {
		IMPORT, QUERY, SUBSET, RESET, STATUS, ERROR, TABLES, LIST_TABLE_SCHEMAS, TABLE_DEFINITION, SCHEME, CHECK, DEFAULT_CONFIG, DROP_TABLE
	}

	private static void logMessage(String msg) {
		if (!verbose)
			return;
		LOG.info(msg);
	}

	public static void main(String[] args) throws SQLException {
		long time = System.currentTimeMillis();
		String dbUrl;
		String user;
		String password;
		String dbName;
		String serverName;
		String pgSchema;
		String msg;
		boolean updateMode = false;

		boolean error = false;
		Mode mode = Mode.ERROR;

		Options options = getOptions();

		// What has to be done
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			LOG.error("Can't parse arguments: " + e.getMessage());
			printHelp(options);
			System.exit(1);
		}

		verbose = cmd.hasOption('v');
		if (verbose)
			LOG.info("Verbose logging enabled.");

		// selecting the mode
		if (cmd.hasOption("h"))
			error = true; // To show help
		if (cmd.hasOption("i"))
			mode = Mode.IMPORT;
		if (cmd.hasOption("u")) {
			mode = Mode.IMPORT;
			updateMode = true;
		}
		if (cmd.hasOption("q"))
			mode = Mode.QUERY;
		if (cmd.getOptionValue("s") != null)
			mode = Mode.SUBSET;
		if (cmd.getOptionValue("re") != null)
			mode = Mode.RESET;
		if (cmd.getOptionValue("st") != null)
			mode = Mode.STATUS;
		if (cmd.hasOption("t"))
			mode = Mode.TABLES;
		if (cmd.hasOption("lts"))
			mode = Mode.LIST_TABLE_SCHEMAS;
		if (cmd.hasOption("td"))
			mode = Mode.TABLE_DEFINITION;
		if (cmd.hasOption("sch"))
			mode = Mode.SCHEME;
		if (cmd.hasOption("ch"))
			mode = Mode.CHECK;
		if (cmd.hasOption("dc"))
			mode = Mode.DEFAULT_CONFIG;
		if (cmd.hasOption("dt"))
			mode = Mode.DROP_TABLE;

		// authentication
		// config file
		String dbcConfigPath = findConfigurationFile();
		if (cmd.hasOption("dbc"))
			dbcConfigPath = cmd.getOptionValue("dbc");
		File conf = new File(dbcConfigPath);
		dbUrl = cmd.getOptionValue('U');
		if (dbUrl == null) {
			msg = "No database URL given. Using value in configuration file";
			logMessage(msg);
		}
		user = cmd.getOptionValue("n");
		if (user == null) {
			msg = "No database username given. Using value in configuration file";
			logMessage(msg);
		}
		password = cmd.getOptionValue("p");
		if (password == null) {
			msg = "No password given. Using value in configuration file";
			logMessage(msg);
		}
		serverName = cmd.getOptionValue("srv");
		dbName = cmd.getOptionValue("db");
		pgSchema = cmd.getOptionValue("pgs");
		if (!((serverName != null && dbName != null) ^ dbUrl != null)
				&& !(serverName == null && dbName == null && dbUrl == null) && !conf.exists()) {
			LOG.error(
					"No base configuration has been found. Thus, you must specify server name and database name or the complete URL with -u (but not both).");
			System.exit(1);
		}

		DataBaseConnector dbc = null;
		try {
			if (conf.exists()) {
				logMessage(String.format("Using configuration file at %s", conf));
				if (dbUrl == null)
					dbc = new DataBaseConnector(serverName, dbName, user, password, pgSchema,
							new FileInputStream(conf));
				else
					dbc = new DataBaseConnector(dbUrl, user, password, pgSchema, new FileInputStream(conf));
			} else {
				logMessage(String.format(
						"No custom configuration found (should be located at %s). Using default configuration.",
						Stream.of(USER_SCHEME_DEFINITION).collect(Collectors.joining(" or "))));
				if (dbUrl == null)
					dbc = new DataBaseConnector(serverName, dbName, user, password, pgSchema, null);
				else
					dbc = new DataBaseConnector(dbUrl, user, password, pgSchema, null);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// all those options...
		String tableName = cmd.getOptionValue("td");
		if (tableName == null)
			tableName = cmd.getOptionValue("ch");

		String subsetTableName = cmd.getOptionValue("s");
		if (subsetTableName == null)
			subsetTableName = cmd.getOptionValue("re");
		if (subsetTableName == null)
			subsetTableName = cmd.getOptionValue("renp");
		if (subsetTableName == null)
			subsetTableName = cmd.getOptionValue("st");

		String fileStr = cmd.getOptionValue("f");
		if (fileStr == null)
			fileStr = cmd.getOptionValue("i");
		if (fileStr == null)
			fileStr = cmd.getOptionValue("u");

		String superTableName = cmd.getOptionValue("z");
		if (superTableName == null)
			superTableName = dbc.getActiveDataTable();

		String queryStr = cmd.getOptionValue("q");
		String subsetJournalFileName = cmd.getOptionValue("j");
		String subsetQuery = cmd.getOptionValue("o");
		String randomSubsetSize = cmd.getOptionValue("r");
		String whereClause = cmd.getOptionValue("w");
		String xpath = cmd.getOptionValue("x");
		String baseOutDir = cmd.getOptionValue("out");
		String batchSize = cmd.getOptionValue("bs");
		String limit = cmd.getOptionValue("l");
		String tableSchema = cmd.getOptionValue("ts") != null ? cmd.getOptionValue("ts") : dbc.getActiveTableSchema();
		boolean useDelimiter = baseOutDir != null ? false : cmd.hasOption("d");
		boolean returnPubmedArticleSet = cmd.hasOption("pas");
		boolean mirrorSubset = cmd.hasOption("m");
		boolean all4Subset = cmd.hasOption("a");
		Integer numberRefHops = cmd.hasOption("rh") ? Integer.parseInt(cmd.getOptionValue("rh")) : null;
		String comment = null;

		if (tableSchema.matches("[0-9]+")) {
			tableSchema = dbc.getConfig().getTableSchemaNames().get(Integer.parseInt(tableSchema));
		}

		switch (mode) {
		case QUERY:
			QueryOptions qo = new QueryOptions();
			qo.fileStr = fileStr;
			qo.queryStr = queryStr;
			qo.useDelimiter = useDelimiter;
			qo.pubmedArticleSet = returnPubmedArticleSet;
			qo.xpath = xpath;
			qo.baseOutDirStr = baseOutDir;
			qo.batchSizeStr = batchSize;
			qo.limitStr = limit;
			qo.tableName = superTableName;
			qo.tableSchema = tableSchema;
			qo.whereClause = whereClause;
			qo.numberRefHops = numberRefHops;
			error = doQuery(dbc, qo);
			break;

		case IMPORT:
			error = doImportOrUpdate(dbc, fileStr, queryStr, superTableName, comment, updateMode);
			break;

		case SUBSET:
			error = doSubset(dbc, subsetTableName, fileStr, queryStr, superTableName, subsetJournalFileName,
					subsetQuery, mirrorSubset, whereClause, all4Subset, randomSubsetSize, numberRefHops, comment);
			break;

		case RESET:
			if (subsetTableName == null) {
				LOG.error("You must provide the name of the subset table to reset.");
				error = true;
			} else {
				boolean files = cmd.hasOption("f");
				try {
					if (!files || StringUtils.isBlank(fileStr)) {
						boolean np = cmd.hasOption("np");
						boolean ne = cmd.hasOption("ne");
						String lc = cmd.hasOption("lc") ? cmd.getOptionValue("lc") : null;
						if (np)
							logMessage("table reset is restricted to non-processed table rows");
						if (ne)
							logMessage("table reset is restricted to table row without errors");
						if (lc != null)
							logMessage("table reset is restricted to rows with last component " + lc);
						if (!np && !ne && lc == null) {
							SubsetStatus status = dbc.status(subsetTableName);
							long inProcess = status.inProcess;
							long isProcessed = status.isProcessed;
							long total = status.total;
							// We don't bother with too small datasets, worst
							// case would be to do it again for 10000 docs which
							// is not much.
							if (total > 10000 && inProcess + isProcessed >= total / 2) {
								String input = getYesNoAnswer("The subset table \"" + subsetTableName
										+ "\" is in process or already processed over 50%."
										+ " Do you really wish to reset it completely into an unprocessed state? (yes/no)");
								if (input.equals("no"))
									System.exit(0);
							}
						}
						dbc.resetSubset(subsetTableName, np, ne, lc);
					} else {
						logMessage("Resetting all documents identified by the IDs in file \"" + fileStr + "\".");
						try {
							List<Object[]> pkValues = asListOfArrays(fileStr);
							dbc.resetSubset(subsetTableName, pkValues);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} catch (TableNotFoundException e) {
					e.printStackTrace();
				}
			}
			break;
		case STATUS:
			error = doStatus(dbc, subsetTableName);
			break;

		case TABLES:
			for (String s : dbc.getTables())
				System.out.println(s);
			break;

		case TABLE_DEFINITION:
			for (String s : dbc.getTableDefinition(tableName))
				System.out.println(s);
			break;

		case LIST_TABLE_SCHEMAS:
			System.out.println("The following table schema names are contained in the current configuration:\n");
			List<String> tableSchemaNames = dbc.getConfig().getTableSchemaNames();
			IntStream.range(0, tableSchemaNames.size()).mapToObj(i -> i + " " + tableSchemaNames.get(i))
					.forEach(System.out::println);
			break;

		case SCHEME:
			System.out.println(dbc.getScheme());
			break;

		case CHECK:
			dbc.checkTableDefinition(tableName);
			break;

		case DEFAULT_CONFIG:
			System.out.println(new String(dbc.getEffectiveConfiguration()));
			break;

		case DROP_TABLE:
			dropTableInteractively(dbc, cmd.getOptionValue("dt"));
			break;

		case ERROR:
			break;
		}

		if (error) {
			// printHelp(options);
			System.exit(1);
		}

		time = System.currentTimeMillis() - time;
		LOG.info(String.format("Processing took %d seconds.", time / 1000));
	}

	public static String findConfigurationFile() {
		File workingDirectory = new File(".");
		Set<String> possibleConfigFileNames = new HashSet<>(Arrays.asList(USER_SCHEME_DEFINITION));
		for (String file : workingDirectory.list()) {
			if (possibleConfigFileNames.contains(file.toLowerCase()))
				return file;
		}
		return "<none found>";
	}

	private static void dropTableInteractively(DataBaseConnector dbc, String tableName) {
		try {
			if (!dbc.tableExists(tableName)) {
				if (tableName.contains("."))
					System.err
							.println("Table \"" + tableName + "\" does not exist in database " + dbc.getDbURL() + ".");
				else
					System.err.println("Table \"" + tableName + "\" does not exist in database " + dbc.getDbURL()
							+ " in active schema " + dbc.getActivePGSchema() + ".");
				return;
			} else {
				String unqualifiedTableName = tableName.contains(".") ? tableName.substring(tableName.indexOf(".") + 1)
						: tableName;
				String schema = tableName.contains(".") ? tableName.substring(0, tableName.indexOf("."))
						: dbc.getActivePGSchema();
				System.out.println("Found table \"" + unqualifiedTableName + "\" in schema " + schema + " in database "
						+ dbc.getDbURL() + ". Do you really want to drop it (y/n)?");
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				String response = in.readLine().toLowerCase();
				while (!response.equals("y") && !response.equals("yes") && !response.equals("n")
						&& !response.equals("no")) {
					System.out.println("Please specify y(es) or n(o).");
					response = in.readLine().toLowerCase();
				}
				if (response.startsWith("y")) {
					System.out.println("Dropping table \"" + unqualifiedTableName + "\" in Postgres schema \"" + schema
							+ "\" of database " + dbc.getDbURL());
					dbc.dropTable(String.join(".", schema, unqualifiedTableName));
				} else {
					System.out.println("User canceled. Aborting process.");
				}
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Poses <tt>question</tt> to the user and awaits for a <tt>yes</tt> or
	 * <tt>no</tt> answer and returns it.
	 * 
	 * @param question
	 *            the question raised
	 * @return the answer <tt>yes</tt> or <tt>no</tt>
	 */
	private static String getYesNoAnswer(String question) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input = "";
		try {
			while (!input.equals("yes") && !input.equals("no")) {
				System.out.println(question);
				input = br.readLine();
			}
		} catch (IOException e) {
			LOG.error("Something went wrong while reading from STDIN: ", e);
			System.exit(1);
		}
		return input;
	}

	private static boolean doStatus(DataBaseConnector dbc, String subsetTableName) {
		boolean error = false;
		try {
			if (subsetTableName == null) {
				LOG.error("You must provide the name of a subset table to display its status.");
				error = true;
			} else {
				SubsetStatus status = dbc.status(subsetTableName);
				System.out.println(status);
			}
		} catch (TableSchemaDoesNotExistException e) {
			LOG.error(e.getMessage());
			error = true;
		} catch (TableNotFoundException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
		return error;
	}

	private static boolean doSubset(DataBaseConnector dbc, String subsetTableName, String fileStr, String queryStr,
			String superTableName, String subsetJournalFileName, String subsetQuery, boolean mirrorSubset,
			String whereClause, boolean all4Subset, String randomSubsetSize, Integer numberRefHops, String comment)
			throws SQLException {
		boolean error = false;
		ArrayList<String> ids = null;
		String condition = null;

		error = checkSchema(dbc, subsetTableName);
		if (!error) {
			if (subsetJournalFileName != null) {
				try {
					ids = asList(subsetJournalFileName);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (ids.size() == 0) {
					LOG.error(subsetJournalFileName + " is empty.");
					error = true;
				}
				StringBuilder sb = new StringBuilder();
				for (String id : ids)
					sb.append(", ").append(id);
				condition = Constants.NLM_ID_FIELD_NAME;
				comment = "Subset created " + new Date().toString() + " by matching with " + superTableName + " on "
						+ condition + ": " + sb.substring(2);
			} else if (subsetQuery != null) {
				logMessage("Querying PubMed for: " + subsetQuery);
				ids = QueryPubMed.query(subsetQuery);
				if (ids.size() == 0) {
					LOG.error("No results for your query.");
					error = true;
				} else
					LOG.info("PubMed delivered " + ids.size() + " results.");
				condition = Constants.PMID_FIELD_NAME;
				comment = "Subset created " + new Date().toString() + " by matching with " + superTableName
						+ " on PubMed-query: " + subsetQuery;
			} else if (all4Subset) {
				logMessage("Creating subset by matching all entries from table " + superTableName + ".");
				comment = "Subset created " + new Date().toString() + " by matching with " + superTableName;
			} else if (whereClause != null) {
				comment = "Subset created " + new Date().toString() + " by selecting rows from " + superTableName
						+ " with where clause \"" + whereClause + "\"";
			} else if (randomSubsetSize != null) {
				try {
					new Integer(randomSubsetSize);
					comment = "Subset created " + new Date().toString() + " by randomly selecting " + randomSubsetSize
							+ " rows from " + superTableName + ".";
				} catch (NumberFormatException e) {
					LOG.error(randomSubsetSize + " is not a number!");
					error = true;
				}
			} else if (fileStr != null) {
				try {
					ids = asList(fileStr);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (ids.size() == 0) {
					LOG.error(fileStr + " is empty.");
					error = true;
				}
				condition = dbc.getFieldConfiguration(dbc.getActiveTableSchema()).getPrimaryKey()[0];
				comment = "Subset created " + new Date().toString() + " by matching with " + superTableName + " on "
						+ ids.size() + " " + condition + "s;";
			} else if (mirrorSubset) {
				comment = "Subset created " + new Date().toString() + " as to mirror " + superTableName + ";";
			} else {
				error = true;
				LOG.error("You must choose a way to define the subset.");
			}

			comment = escapeSingleQuotes(comment);
		}
		if (!dbc.tableExists(superTableName)) {
			logMessage("Checking whether super table " + superTableName + " exists...");
			LOG.error("Table " + superTableName + " doesn't exist!");
			error = true;
		}
		if (!error) {
			if (!dbc.tableExists(subsetTableName)) {
				logMessage("No table with the name \"" + subsetTableName + "\" exists, creating new subset table...");
				dbc.createSubsetTable(subsetTableName, superTableName, numberRefHops, comment);
				logMessage("Created table " + subsetTableName);
			} else
				LOG.error("Table " + subsetTableName + " allready exists.");
			if (dbc.isEmpty(subsetTableName) && !error) {
				if (all4Subset)
					dbc.initSubset(subsetTableName, superTableName);
				else if (whereClause != null)
					dbc.initSubsetWithWhereClause(subsetTableName, superTableName, whereClause);
				else if (ids != null && ids.size() > 0)
					dbc.initSubset(ids, subsetTableName, superTableName, condition);
				else if (mirrorSubset)
					dbc.initMirrorSubset(subsetTableName, superTableName, true);
				else if (randomSubsetSize != null) {
					dbc.initRandomSubset(new Integer(randomSubsetSize), subsetTableName, superTableName);
				}
				logMessage("Subset defined.");
			} else {
				LOG.error(subsetTableName + " is not empty, please use another table.");
				error = true;
			}
		}
		return error;
	}

	private static boolean doImportOrUpdate(DataBaseConnector dbc, String fileStr, String queryStr,
			String superTableName, String comment, boolean updateMode) throws SQLException {
		boolean error = false;
		if (fileStr != null) {

			if (!dbc.tableExists(superTableName)) {
				error = checkSchema(dbc, superTableName);
				comment = "Data table created " + new Date().toString() + " by importing data from path " + fileStr;
				if (!error) {
					dbc.createTable(superTableName, comment);
					logMessage("Created table " + superTableName);

				}
			}

			if (dbc.isEmpty(superTableName) && !updateMode) {
				dbc.importFromXMLFile(fileStr, superTableName);
			} else {
				logMessage("Table is not empty or update mode was explicitly specified, processing Updates.");
				dbc.updateFromXML(fileStr, superTableName);
				logMessage("Updates finished.");
			}
		} else {
			LOG.error("You must specify a file or directory to retrieve XML files from.");
			error = true;
		}
		return error;
	}

	private static boolean doQuery(DataBaseConnector dbc, QueryOptions qo) {
		boolean error = false;

		/**
		 * The document IDs that should be returned (optional)
		 */
		String queryStr = qo.queryStr;
		String fileStr = qo.fileStr;
		String tableName = qo.tableName;
		String tableSchema = qo.tableSchema;
		boolean useDelimiter = qo.useDelimiter;
		boolean pubmedArticleSet = qo.pubmedArticleSet;
		String xpath = qo.xpath;
		// this could be a directory or file name, depending on parameters
		String baseOutFile = qo.baseOutDirStr;
		String batchSizeStr = qo.batchSizeStr;
		String limitStr = qo.limitStr;
		Integer numberRefHops = qo.numberRefHops;

		// In the following algorithm, first of all each possible
		// parameter/resource is acquired. Further down is then one single
		// algorithm iterating over queried documents and treating them
		// accordingly to the parameters which have been found.
		File outfile = null;
		int batchSize = 0;
		BufferedWriter bw = null;
		boolean keysExplicitlyGiven = fileStr != null || queryStr != null;
		long limit = limitStr != null ? Integer.parseInt(limitStr) : -1;

		boolean createDirectory = baseOutFile != null && !pubmedArticleSet;
		if (verbose) {
			logMessage("Creating " + (createDirectory ? "directory" : "file") + " " + baseOutFile
					+ " to write query results to.");
		}

		if (createDirectory) {
			outfile = new File(baseOutFile);
			if (!outfile.exists()) {
				logMessage("Directory " + outfile.getAbsolutePath()
						+ " does not exist and will be created (as well as sub dircetories for file batches if required).");
				outfile.mkdir();
			}
			logMessage("Writing queried documents to " + outfile.getAbsolutePath());

			if (batchSizeStr != null) {
				try {
					batchSize = Integer.parseInt(batchSizeStr);
					logMessage("Dividing query result files in batches of " + batchSize);
					if (batchSize < 1)
						throw new NumberFormatException();
				} catch (NumberFormatException e) {
					LOG.error(
							"Error parsing \"{}\" into an integer. Please deliver a positive numeric value for the batch size of files.");
				}
			}
		}

		if (!error) {
			List<Object[]> keys = new ArrayList<Object[]>();
			if (fileStr != null) {
				try {
					keys = asListOfArrays(fileStr);
				} catch (IOException e1) {
					LOG.error("Could not open '" + new File(fileStr).getAbsolutePath() + "'.");
					error = true;
				}
			}
			if (queryStr != null) {
				for (String pmid : queryStr.split(","))
					keys.add(pmid.split(KEY_PART_SEPERATOR));
			}

			// Main algorithm iterating over documents.
			try {
				if (!error) {
					Iterator<byte[][]> it = null;
					if (!keysExplicitlyGiven) {
						it = dbc.querySubset(tableName, qo.whereClause, limit, numberRefHops, tableSchema);
					} else if (keys.size() > 0)
						it = dbc.queryIDAndXML(keys, tableName);
					else
						throw new IllegalStateException(
								"No query keys have been explicitly given (e.g. in a file) nor should the whole table be queried.");
					int i = 0;
					// The name of the sub directories will just be their batch
					// number. We start at -1 because the batchNumber will be
					// incremented first of all (0 % x == 0, Ax).
					int batchNumber = -1;
					// outDir will be baseOutDir plus the current batch number
					// of files when
					// saving the queried files in separate batches is wished.
					File outDir = outfile;

					if (pubmedArticleSet) {
						if (null != baseOutFile) {
							logMessage(
									"Creating a single file with a PubmedArticleSet and writing it to " + baseOutFile);
							bw = new BufferedWriter(new FileWriter(baseOutFile));
						}
						print("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
								+ "<!DOCTYPE PubmedArticleSet SYSTEM \"http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_170101.dtd\">\n"
								+ "<PubmedArticleSet>", bw);
					}

					while (it.hasNext()) {
						byte[][] idAndXML = it.next();
						if (outfile != null) {
							// if we want batches, create appropriate
							// subdirectories
							if (batchSize > 0 && i % batchSize == 0) {
								++batchNumber;
								// Adjust the sub directory for the new batch.
								String subDirectoryName = (batchNumber > -1 && batchSize > 0
										? Integer.toString(batchNumber) : "");
								String subDirPath = outfile.getAbsolutePath() + FILE_SEPERATOR + subDirectoryName;
								outDir = new File(subDirPath);
								outDir.mkdir();
							}

							// Write the current file into the given directory
							// and use the key as file name
							String filename = new String(idAndXML[0]);

							if (!pubmedArticleSet) {
								if (bw != null)
									bw.close();
								bw = new BufferedWriter(new FileWriter(outDir + FILE_SEPERATOR + filename));
							}
						}
						if (xpath == null) {
							StringBuilder sb = new StringBuilder();
							if (pubmedArticleSet)
								sb.append("<PubmedArticle>\n");
							sb.append(new String(idAndXML[1], "UTF-8"));
							if (pubmedArticleSet)
								sb.append("\n</PubmedArticle>");
							print(sb.toString(), bw);
						} else {
							// 'values' contains for each XPath delivered one
							// array of Strings holding the values for this
							// XPath (e.g. the AuthorList mostly yields several
							// values).
							String[][] values = getXpathValues(idAndXML[1], xpath);
							for (String[] valuesOfXpath : values)
								for (String singleValue : valuesOfXpath)
									print(singleValue, bw);
						}
						if (useDelimiter)
							System.out.println(DELIMITER);
						++i;

					}

					if (pubmedArticleSet) {
						print("</PubmedArticleSet>", bw);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (bw != null)
						bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return error;
	}

	/**
	 * @param string
	 * @param bw
	 * @throws IOException
	 */
	private static void print(String string, BufferedWriter bw) throws IOException {
		if (bw == null)
			System.out.println(string);
		else
			bw.write(string + "\n");
	}

	private static String[][] getXpathValues(byte[] next, String xpaths) {

		String[] xpathArray = xpaths.split(",");
		List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
		for (String xpath : xpathArray) {
			Map<String, String> field = new HashMap<String, String>();
			field.put(JulieXMLConstants.NAME, xpath);
			field.put(JulieXMLConstants.XPATH, xpath);
			field.put(JulieXMLConstants.RETURN_XML_FRAGMENT, "true");
			field.put(JulieXMLConstants.RETURN_ARRAY, "true");
			fields.add(field);
		}

		String[][] retStrings = new String[xpathArray.length][];

		Iterator<Map<String, Object>> it = JulieXMLTools.constructRowIterator(next, 1024, ".", fields, "your result");
		if (it.hasNext()) {
			Map<String, Object> row = it.next();
			for (int i = 0; i < xpathArray.length; i++) {
				// Get the field "xpath" which was given as field name above; we
				// wanted multiple results to be returned in an array.
				String[] values = (String[]) row.get(xpathArray[i]);
				if (values == null)
					values = new String[] { "XPath " + xpaths + " does not exist in this document." };
				retStrings[i] = values;
			}
			if (it.hasNext()) {
				// What happened? We wanted all values in one array, so this
				// should not happen.
				LOG.warn(
						"There are more results for the XPath {} then expected and not all have been returned. Please contact a developer for help.",
						xpaths);
			}
		}
		return retStrings;
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(160);
		formatter.printHelp(CLI.class.getName(), options);
	}

	private static Options getOptions() {
		Options options = new Options();

		// -------------------- OptionGroup for available modes --------------
		OptionGroup modes = new OptionGroup();

		modes.addOption(buildOption("i", "import", "Import data into the _data table", "file/dir to import"));
		modes.addOption(buildOption("u", "update", "Update _data table", "file/dir to update from"));
		modes.addOption(buildOption("s", "subset",
				"Define a subset table; use -f, -o, -a, -m, -w or -r to specify the subsets source.",
				"name of the new subset table"));
		modes.addOption(buildOption("re", "reset",
				"Resets a subset table to a not-yet-processed state. Flags:\n" + "-np only reset non-processed items\n"
						+ "-ne only reset items without errors\n"
						+ "-lc to reset only those items with the given last component\n"
						+ "-f a partial reset can be achieved by specifying a file containing one primary key value for each document to be resetted",
				"subset table name"));
		modes.addOption(
				buildOption("st", "status", "Show the processing status of a subset table.", "subset table name"));

		OptionBuilder.withLongOpt("query");
		OptionBuilder.withDescription("Query a table (default: " + Constants.DEFAULT_DATA_TABLE_NAME
				+ ") for XMLs. You can enter the primary keys directly or use -f to specify a file. If you define none of both, the whole table will be returned.\n"
				+ "Use -d to display delimiters between the results.\n"
				+ "Use -z to specify the target table. If the table is a subset, only documents in this subset will be returned.\n"
				+ "Use -l to set a limit of returned documents.\n"
				+ "Use -x to specify an XPath expression go extract specific parts of the queried XML documents.\n"
				+ "Use -out to save the query results to file.");
		OptionBuilder.hasOptionalArg();
		OptionBuilder.withArgName("your query");
		modes.addOption(OptionBuilder.create("q"));

		modes.addOption(buildOption("h", "help", "Displays all possible parameters."));
		modes.addOption(buildOption("t", "tables", "Displays all tables in the active scheme."));

		modes.addOption(buildOption("td", "tabledefinition", "Displays the columns of a table.", "the table"));

		modes.addOption(buildOption("ds", "displayscheme", "Displays the active scheme."));
		modes.addOption(buildOption("ch", "check",
				"Checks if a table confirms to its definition (for subsets: only primary keys!)", "table"));
		modes.addOption(buildOption("dc", "defaultconfig", "Prints the defaultConfiguration."));
		modes.addOption(buildOption("dt", "droptable", "Drops the given table.", "table"));

		modes.addOption(buildOption("lts", "listtableschemas",
				"Displays all table schema names in the configuration. The showed name index can be used as value for the -ts option."));

		modes.setRequired(true);

		options.addOptionGroup(modes);

		// -------------------- OptionGroup for exclusive
		// parameters--------------
		OptionGroup exclusive = new OptionGroup();

		exclusive.addOption(buildOption("f", "file",
				"Set the file used for query, subset creation or partial subset reset.", "file"));
		exclusive.addOption(buildOption("o", "online",
				"Defines the subset by a PubMed query - remember to wrap it in double quotation marks!", "query"));
		exclusive.addOption(buildOption("a", "all", "Use all entries of the _data table for the subset."));
		exclusive.addOption(buildOption("r", "random",
				"Generates a random subset, you must provide its size as a parameter. Often used with -z.", "size"));
		exclusive.addOption(buildOption("m", "mirror",
				"Creates a subset table which mirrors the database table. I.e. when the data table gets new records, the mirror subset(s) will be updated accordingly."));
		exclusive
				.addOption(buildOption("w", "where", "Uses a SQL WHERE clause during subset definition.", "condition"));
		exclusive.addOption(
				buildOption("j", "journals", "Define a subset by providing a file with journal names.", "file"));
		exclusive.addOption(
				buildOption("l", "limit", "For use with -q. Restricts the number of documents returned.", "limit"));

		options.addOption(buildOption("np", "not processed",
				"Flag for -re(set) mode to restrict to non-processed table rows. May be combined with -ne, -lc."));
		options.addOption(buildOption("ne", "no errors",
				"Flag for -re(set) mode to restrict to table rows without errors. May be combined with -np, -lc."));
		options.addOption(buildOption("lc", "last component",
				"Option for -re(set) mode to restrict to table rows to a given last component identifier. May be combined with -np, -ne.",
				"component name"));

		options.addOptionGroup(exclusive);

		// --------------- optional details for many modes --------------
		options.addOption(buildOption("z", "superset",
				"Provides a superset name for definition of a subset or the name of a data table.",
				"name of the superset table"));
		options.addOption(buildOption("v", "verbose", "Activate verbose informational ouput of the tool's actions"));

		options.addOption(buildOption("d", "delimiter", "Display a line of \"-\" as delimiter between the results."));
		options.addOption(buildOption("pas", "pubmedarticleset",
				"For use with -q. The queried documents will be interpreted as Medline XML documents and will be enclosed in PubmedArticleSet."));
		options.addOption(buildOption("out", "out",
				"The file or directory where query results are written to. By default, a directory will be created and it will be filled with one file per document. The files will have the name of their database primary key. Modifying parameters:\n"
						+ "Use -bs to create subdirectories for batches of files.\n"
						+ "Use -pas to create no directory but a single XML file representing a PubmedArticleSet. This assumes that the queried documents are Medline or Pubmed XML documents.",
				"output directory"));
		options.addOption(buildOption("bs", "batchsize",
				"The number of queried documents (by -q and -out) which should be written in one directory. Subdirectories will be created at need.",
				"batchsize"));
		options.addOption(buildOption("x", "xpath",
				"When querying documents using -q, you may specify one or more XPath expressions to restrict the output to the elements referenced by your XPath expressions. Several XPaths must be delimited by a single comma.",
				"xpath"));
		options.addOption(buildOption("rh", "referencehops",
				"The maximum number of allowed hops to tables referenced with a foreign key when creating subset tables.",
				"max number of hops"));
		options.addOption(buildOption("ts", "tableschema",
				"Table Schema to use; currently only supported by -q mode. The name can be given or the index as retrieved by the -lts mode.",
				"schemaname"));

		// -------------------- authentication --------------------
		options.addOption(buildOption("U", "url",
				"URL to database server (e.g. jdbc:postgresql://<host name>/<db name>)", "url"));
		options.addOption(buildOption("n", "username", "username for database", "username"));
		options.addOption(buildOption("p", "pass", "password for database", "password"));
		options.addOption(buildOption("pgs", "pgschema", "Postgres Schema to use", "schema"));
		options.addOption(buildOption("srv", "server", "Server name to connect to", "servername"));
		options.addOption(buildOption("db", "database", "Database to connect to", "database"));
		options.addOption(buildOption("dbc", "databaseconfiguration",
				"XML file specifying the user configuration (defaults to dbcConfiguration.xml).", "Config File"));

		return options;
	}

	private static Option buildOption(String shortName, String longName, String description, String... arguments) {
		OptionBuilder.withLongOpt(longName);
		OptionBuilder.withDescription(description);
		OptionBuilder.hasArgs(arguments.length);
		for (String argument : arguments)
			OptionBuilder.withArgName(argument);
		return OptionBuilder.create(shortName);
	}

	/**
	 * 
	 * @param dbc
	 *            - databaseconnector
	 * @param tableName
	 *            - name of the table to check
	 * @return true - if there was an error, otherwise false
	 */
	private static boolean checkSchema(DataBaseConnector dbc, String tableName) {
		boolean error = false;
		String[] tablePath = tableName.split("\\.");
		// if the table name has the form 'schemaname.tablename'
		if (tablePath.length == 2 && !dbc.schemaExists(tablePath[0]))
			dbc.createSchema(tablePath[0]);
		else if (tablePath.length > 2) {
			LOG.error(String.format(
					"The table path %s is invalid. Only table names of the form 'tablename' or 'schemaname.tablename'are accepted.",
					tableName));

		}
		return error;
	}

	private static String escapeSingleQuotes(String comment) {
		return comment.replaceAll("'", "\\\\'");

	}

	private static List<Object[]> asListOfArrays(String fileStr) throws IOException {
		List<Object[]> list = new ArrayList<Object[]>();
		File file = new File(fileStr);
		if (file != null) {
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				String line = br.readLine();
				while (line != null) {
					list.add(line.split(KEY_PART_SEPERATOR));
					line = br.readLine();
				}
			}
		}
		return list;
	}

	private static ArrayList<String> asList(String fileStr) throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		File file = new File(fileStr);
		if (file != null) {
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				String line = br.readLine();
				while (line != null) {
					list.add(line);
					line = br.readLine();
				}
			}
		}
		return list;
	}
}
