/**
 * QueryCLI.java
 * <p>
 * Copyright (c) 2010, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 * <p>
 * Author: faessler
 * <p>
 * Current version: 1.0
 * Since version:   1.0
 * <p>
 * Creation date: 20.11.2010
 **/

package de.julielab.costosys.cli;

import de.julielab.costosys.Constants;
import de.julielab.costosys.configuration.TableSchemaDoesNotExistException;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.costosys.dbconnection.SubsetStatus;
import de.julielab.costosys.dbconnection.util.CoStoSysException;
import de.julielab.costosys.dbconnection.util.CoStoSysRuntimeException;
import de.julielab.costosys.medline.ConfigurationConstants;
import de.julielab.costosys.medline.Updater;
import de.julielab.java.utilities.IOStreamUtilities;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static de.julielab.costosys.dbconnection.DataBaseConnector.StatusElement;
import static de.julielab.costosys.dbconnection.DataBaseConnector.StatusElement.*;

/**
 * Command line interface for interaction with a databases holding e.g. Medline
 * XML data.
 *
 * @author faessler / hellrich
 */
public class CLI {

    private final static String DELIMITER = "\n--------------------------------------------------------------------------------\n";

    private static final Logger LOG = LoggerFactory.getLogger(CLI.class);
    private static final String KEY_PART_SEPERATOR = "\t";
    private static final String FILE_SEPERATOR = System.getProperty("file.separator");
    public static String[] USER_SCHEME_DEFINITION = new String[]{"dbcconfiguration.xml", "costosys.xml", "costosysconfiguration.xml"};
    private static boolean verbose = false;

    private static void logMessage(String msg) {
        if (!verbose)
            return;
        LOG.info(msg);
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        String dbUrl;
        String user;
        String password;
        String dbName;
        String serverName;
        String pgSchema;
        String msg;
        boolean updateMode = false;

        Mode mode = Mode.ERROR;

        Options options = CliOptionsProvider.getOptions();

        // What has to be done
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);


            verbose = cmd.hasOption('v');
            if (verbose)
                LOG.info("Verbose logging enabled.");

            // selecting the mode
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
            if (cmd.hasOption("im"))
                mode = Mode.IMPORT_UPDATE_MEDLINE;
            if (cmd.hasOption("mp"))
                mode = Mode.MARK_PROCESSED;
            if (cmd.hasOption("vn"))
                mode = Mode.PRINT_VERSION;

            if (mode == Mode.PRINT_VERSION) {
                printVersion();
                return;
            }

            // authentication
            // configuration file
            String dbcConfigPath = null;
            if (cmd.hasOption("dbc"))
                dbcConfigPath = cmd.getOptionValue("dbc");
            if (dbcConfigPath == null)
                dbcConfigPath = findConfigurationFile();
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
                final String errorMsg = "No base configuration has been found. Thus, you must specify server name and database name or the complete URL with -u (but not both).";
                LOG.error(
                        errorMsg);
                throw new IllegalArgumentException(errorMsg);
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
            final String copyProcessed = cmd.getOptionValue("cp");

            if (tableSchema.matches("[0-9]+")) {
                tableSchema = dbc.getConfig().getTableSchemaNames().get(Integer.parseInt(tableSchema));
            }
            dbc.setActiveTableSchema(tableSchema);

            try (CoStoSysConnection ignored = dbc.obtainOrReserveConnection(true)) {
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
                        doQuery(dbc, qo);
                        break;

                    case IMPORT:
                        doImportOrUpdate(dbc, fileStr, superTableName, updateMode);
                        break;

                    case SUBSET:
                        doSubset(dbc, subsetTableName, fileStr, queryStr, superTableName, subsetJournalFileName,
                                subsetQuery, mirrorSubset, whereClause, all4Subset, randomSubsetSize, numberRefHops, copyProcessed);
                        break;

                    case RESET:
                        if (subsetTableName == null) {
                            LOG.error("You must provide the name of the subset table to reset.");
                        } else {
                            boolean files = cmd.hasOption("f");
                            try {
                                boolean doReset = true;
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
                                        SubsetStatus status = dbc.status(subsetTableName, EnumSet.of(IN_PROCESS, IS_PROCESSED, TOTAL));
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
                                            if (!"yes".equals(input))
                                                doReset = false;
                                        }
                                    }
                                    if (doReset)
                                        dbc.resetSubset(subsetTableName, np, ne, lc);
                                } else {
                                    logMessage("Resetting all documents identified by the IDs in file \"" + fileStr + "\".");
                                    try {
                                        List<Object[]> pkValues = convertFileToPrimaryKeyList(fileStr);
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
                        doStatus(dbc,
                                subsetTableName,
                                cmd.hasOption("he"),
                                cmd.hasOption("isp"),
                                cmd.hasOption("inp"),
                                cmd.hasOption("to"),
                                cmd.hasOption("lc"));
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

                    case IMPORT_UPDATE_MEDLINE:
                        Updater updater = new Updater(loadXmlConfiguration(new File(cmd.getOptionValue("im"))));
                        updater.process(dbc);
                        break;

                    case MARK_PROCESSED:
                        setProcessed(dbc, cmd.getOptionValue("mp"), fileStr);
                        break;

                    case ERROR:
                        break;
                }
            }

            time = System.currentTimeMillis() - time;
            LOG.info(String.format("Processing took %d seconds.", time / 1000));
        } catch (ParseException e) {
            LOG.error("Can't parse arguments: " + e.getMessage());
            printHelp(options);
        }
    }

    private static void printVersion() throws IOException {
        try (InputStream versionFileStream = CLI.class.getResourceAsStream("/version.txt")) {
            String version = IOStreamUtilities.getStringFromInputStream(versionFileStream).trim();
            System.out.println(version);
        }
    }

    private static void setProcessed(DataBaseConnector dbc, String subsetTable, String fileStr) throws IOException {
        if (!dbc.tableExists(subsetTable)) {
            LOG.error("The subset table {} does not exist.", subsetTable);
            return;
        }
        if (fileStr != null) {
            final File idsFile = new File(fileStr);
            if (!idsFile.exists()) {
                LOG.error("The ID list file {} does not exist.", fileStr);
                return;
            }
            final List<Object[]> primaryKeys = convertFileToPrimaryKeyList(fileStr);
            dbc.markAsProcessed(subsetTable, primaryKeys);
        } else {
            dbc.markAsProcessed(subsetTable);
        }
    }

    public static String findConfigurationFile() throws ConfigurationNotFoundException {
        String configFileProperty = System.getProperty(Constants.COSTOSYS_CONFIG_FILE);
        if (configFileProperty != null && new File(configFileProperty).exists())
            return configFileProperty;
        File workingDirectory = new File(".");
        Set<String> possibleConfigFileNames = new HashSet<>(Arrays.asList(USER_SCHEME_DEFINITION));
        for (String file : workingDirectory.list()) {
            if (possibleConfigFileNames.contains(file.toLowerCase()))
                return file;
        }
        throw new ConfigurationNotFoundException("No configuration file with a name in " + Arrays.toString(USER_SCHEME_DEFINITION) + " was found in the current working directory " + new File(".").getAbsolutePath());
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
                while (!"y".equals(response) && !"yes".equals(response) && !"n".equals(response)
                        && !"no".equals("no")) {
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
     * @param question the question raised
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
        }
        return input;
    }

    private static boolean doStatus(DataBaseConnector dbc, String subsetTableName, boolean showHasErrors, boolean showIsProcessed, boolean showIsInProcess, boolean showTotal, boolean showLastComponent) {
        boolean error = false;
        try {
            if (subsetTableName == null) {
                LOG.error("You must provide the name of a subset table to display its status.");
                error = true;
            } else {
                EnumSet<StatusElement> modes = EnumSet.noneOf(StatusElement.class);
                if (showHasErrors) modes.add(HAS_ERRORS);
                if (showIsProcessed) modes.add(IS_PROCESSED);
                if (showIsInProcess) modes.add(StatusElement.IN_PROCESS);
                if (showTotal) modes.add(StatusElement.TOTAL);
                if (showLastComponent) modes.add(StatusElement.LAST_COMPONENT);
                if (modes.isEmpty())
                    modes = EnumSet.allOf(StatusElement.class);

                try (CoStoSysConnection ignored = dbc.obtainOrReserveConnection(true)) {
                    SubsetStatus status = dbc.status(subsetTableName, modes);
                    System.out.println(status);
                }
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
                                    String whereClause, boolean all4Subset, String randomSubsetSize, Integer numberRefHops, String copyProcessed)
            throws SQLException, CoStoSysException {
        String comment = "<no comment given>";
        boolean error;
        List<String> ids = null;
        List<Object[]> processedIds = Collections.emptyList();
        String condition = null;

        error = checkSchema(dbc, subsetTableName);
        if (!error) {
            if (copyProcessed != null && !copyProcessed.isBlank()) {
                LOG.info("Retrieving rows marked as processed from subset table {}", copyProcessed);
                processedIds = dbc.getProcessedPrimaryKeys(copyProcessed);
                LOG.info("Retrieved {} processed primary keys", processedIds.size());
            }
            if (subsetJournalFileName != null) {
                try {
                    ids = asList(subsetJournalFileName);
                } catch (IOException e) {
                    throw new CoStoSysRuntimeException(e);
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
                    // this is just to provoke an exception in case randomSubsetSize does not contain an integer
                    Integer.valueOf(randomSubsetSize);
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
                    throw new CoStoSysException(fileStr + " is empty.");
                }
                condition = dbc.getFieldConfiguration(dbc.getActiveTableSchema()).getPrimaryKey()[0];
                comment = "Subset created " + new Date().toString() + " by matching with " + superTableName + " on "
                        + ids.size() + " " + condition + "s;";
            } else if (mirrorSubset) {
                comment = "Subset created " + new Date().toString() + " as to mirror " + superTableName + ";";
            } else {
                LOG.error("You must choose a way to define the subset.");
                error = true;
            }

            comment = escapeSingleQuotes(comment);
        }
        if (!dbc.withConnectionQueryBoolean(c -> dbc.tableExists(superTableName))) {
            logMessage("Checking whether super table " + superTableName + " exists...");
            LOG.error("Table " + superTableName + " doesn't exist!");
            error = true;
        }
        if (!error) {
            try (CoStoSysConnection ignored = dbc.obtainOrReserveConnection(true)) {
                if (!dbc.tableExists(subsetTableName)) {
                    logMessage("No table with the name \"" + subsetTableName + "\" exists, creating new subset table...");
                    dbc.createSubsetTable(subsetTableName, superTableName, numberRefHops, comment);
                    logMessage("Created table " + subsetTableName);
                } else {
                    LOG.error("Table " + subsetTableName + " allready exists.");
                    error = true;
                }
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
        }
        if (!error && !processedIds.isEmpty()) {
            LOG.info("Marking {} rows as processed in the new subset table {}", processedIds.size(), subsetTableName);
            final int numSuccessful = dbc.markAsProcessed(subsetTableName, processedIds);
            LOG.info("{} rows were successfully marked as processed in {}.", numSuccessful, subsetTableName);
        }
        return error;
    }

    private static boolean doImportOrUpdate(DataBaseConnector dbc, String fileStr,
                                            String superTableName, boolean updateMode) {
        boolean error = false;
        if (fileStr != null) {

            if (!dbc.withConnectionQueryBoolean(c -> c.tableExists(superTableName))) {
                error = checkSchema(dbc, superTableName);
                final String comment = "Data table created " + new Date().toString() + " by importing data from path " + fileStr;
                if (!error) {
                    dbc.withConnectionExecute(c -> c.createTable(superTableName, comment));
                    logMessage("Created table " + superTableName);

                }
            }

            if (dbc.withConnectionQueryBoolean(c -> c.isEmpty(superTableName)) && !updateMode) {
                dbc.withConnectionExecute(c -> c.importFromXMLFile(fileStr, superTableName));
            } else {
                logMessage("Table is not empty or update mode was explicitly specified, processing Updates.");
                dbc.withConnectionExecute(c -> c.updateFromXML(fileStr, superTableName, true));
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
                    keys = convertFileToPrimaryKeyList(fileStr);
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
                    Iterator<byte[][]> it;
                    if (!keysExplicitlyGiven) {
                        it = dbc.querySubset(tableName, qo.whereClause, limit, numberRefHops, tableSchema);
                    } else if (keys.size() > 0)
                        it = dbc.retrieveColumnsByTableSchema(keys, tableName, tableSchema);
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

                    BinaryDataHandler binaryDataHandler = null;
                    if (dbc.getActiveTableFieldConfiguration().isBinary()) {
                        binaryDataHandler = new BinaryDataHandler(dbc, "public", Collections.emptySet(), dbc.getConfig().getTypeSystemFiles());
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
                            if (!dbc.getActiveTableFieldConfiguration().isBinary())
                                sb.append(new String(idAndXML[1], "UTF-8"));
                            else
                                sb.append(binaryDataHandler.decodeBinaryXmiData(idAndXML));
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
            } catch (CoStoSysException e) {
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
                    values = new String[]{"XPath " + xpaths + " does not exist in this document."};
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


    /**
     * @param dbc       - databaseconnector
     * @param tableName - name of the table to check
     * @return true - if there was an error, otherwise false
     */
    private static boolean checkSchema(DataBaseConnector dbc, String tableName) {
        boolean error = false;
        String[] tablePath = tableName.split("\\.");
        // if the table name has the form 'schemaname.tablename'
        if (tablePath.length == 2 && !dbc.withConnectionQueryBoolean(c -> c.schemaExists(tablePath[0])))
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

    private static List<Object[]> convertFileToPrimaryKeyList(String fileStr) throws IOException {
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

    public static XMLConfiguration loadXmlConfiguration(File configurationFile) throws ConfigurationException {
        try {
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<XMLConfiguration> configBuilder =
                    new FileBasedConfigurationBuilder<>(XMLConfiguration.class).configure(params
                            .xml()
                            .setFile(configurationFile));
            return configBuilder.getConfiguration();
        } catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
            throw new ConfigurationException(e);
        }
    }

    private enum Mode {
        IMPORT, QUERY, SUBSET, RESET, STATUS, ERROR, TABLES, LIST_TABLE_SCHEMAS, TABLE_DEFINITION, SCHEME, CHECK, DEFAULT_CONFIG, DROP_TABLE, IMPORT_UPDATE_MEDLINE, MARK_PROCESSED, PRINT_VERSION
    }
}
