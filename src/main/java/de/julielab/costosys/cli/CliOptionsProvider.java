package de.julielab.costosys.cli;

import de.julielab.costosys.Constants;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class CliOptionsProvider {
    public static Options getOptions() {
        Options options = new Options();

        // -------------------- OptionGroup for available modes --------------
        addModes(options);

        // -------------------- OptionGroup for exclusive parameters--------------
        addOptionExclusiveParameters(options);

        // ----------------- non-exclusive options meant for specific modes --------
        addSharedParameters(options);


        // --------------- optional details for many modes --------------
        addOptionalSharedParameters(options);

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

    private static void addOptionalSharedParameters(Options options) {
        options.addOption(buildOption("z", "superset",
                "Provides a superset name for definition of a subset or the name of a data table.",
                "name of the superset table"));
        options.addOption(buildOption("v", "verbose", "Activate verbose informational ouput of the tool's actions"));

        options.addOption(buildOption("d", "delimiter", "For use with -q. Display a line of \"-\" as delimiter between the results."));
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
    }

    private static void addSharedParameters(Options options) {
        // for the status report
        options.addOption(buildOption("he", "has errors",
                "Flag for -st(atus) mode to add the 'has errors' statistic to a subset status report."));
        options.addOption(buildOption("isp", "is processed",
                "Flag for -st(atus) mode to add the 'is processed' statistic to a subset status report."));
        options.addOption(buildOption("inp", "is in process",
                "Flag for -st(atus) mode to add the 'is in process' statistic to a subset status report."));
        options.addOption(buildOption("to", "total",
                "Flag for -st(atus) mode to add the 'total' statistic to a subset status report."));
        options.addOption(buildOption("slc", "show last component",
                "Flag for -st(atus) mode to add the 'last component' statistic to a subset status report."));

        // for partial subset resets
        options.addOption(buildOption("np", "not processed",
                "Flag for -re(set) mode to restrict to non-processed table rows. May be combined with -ne, -lc."));
        options.addOption(buildOption("ne", "no errors",
                "Flag for -re(set) mode to restrict to table rows without errors. May be combined with -np, -lc."));
        options.addOption(buildOption("lc", "last component",
                "Option for -re(set) mode to restrict to table rows to a given last component identifier. May be combined with -np, -ne.",
                "component name"));

        // for subset creation
        options.addOption(buildOption("cp", "copyprocessed", "For use with -s. Mark all documents to be processed in the new subset table that are marked as processed in the argument subset table.", "subset table to copy the processed markers from"));
    }

    private static void addOptionExclusiveParameters(Options options) {
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
        exclusive.addOption(
                buildOption("iap", "ignore-already-processed", "For use with -im. Indicates that all update files in the configuration should be processed regardless of whether they are already marked as processed in the database update table."));

        options.addOptionGroup(exclusive);
    }

    private static void addModes(Options options) {
        OptionGroup modes = new OptionGroup();

        modes.addOption(buildOption("i", "import", "Import data into the _data table", "file/dir to import"));
        modes.addOption(buildOption("im", "importmedline", "Import PubMed/MEDLINE data into the _data table. The parameter is an XML file holding information about the PubMed/MEDLINE XML file location in the format downloaded directly from the MEDLINE FTP server. Keeps track of already applied imported files via an internal table", "MEDLINE import/update XML configuration"));
        modes.addOption(buildOption("ip", "importpmc", "Import PubMed Central XML data into the _data table. The parameter is an XML file holding information about the PubMed Central XML file location in the format downloaded directly from the PMC FTP server (bulk download). Keeps track of already applied imported files via an internal table", "PMC import/update XML configuration"));
        modes.addOption(buildOption("u", "update", "Update _data table", "file/dir to update from"));
        modes.addOption(buildOption("s", "subset",
                "Define a subset table; use -f, -o, -a, -m, -w or -r to specify the subsets source. Use -z to specify the referenced data table, defaults to _data.",
                "name of the new subset table"));
        modes.addOption(buildOption("re", "reset",
                "Resets a subset table to a not-yet-processed state. Flags:\n" + "-np only reset non-processed items\n"
                        + "-ne only reset items without errors\n"
                        + "-lc to reset only those items with the given last component\n"
                        + "-f a partial reset can be achieved by specifying a file containing one primary key value for each document to be resetted",
                "subset table name"));
        modes.addOption(
                buildOption("st", "status", "Show the processing status of a subset table. Generates a small report containing the number of processed and total documents of a subset table. " +
                        "The report can be customized using the -he, -isp, -inp, -to and -slc switches", "subset table name"));
        modes.addOption(buildOption("mp", "mark-processed", "Sets the is_processed state of a subset table to true. The -f argument can be used to deliver a file that lists document primary keys, one per line. If such a file is given, only the entries in the file are marked as processed.", "the subset table name"));

        modes.addOption(buildOption("q", "query", "Query a table (default: " + Constants.DEFAULT_DATA_TABLE_NAME
                + ") for XMLs. You can enter the primary keys directly or use -f to specify a file. In this case, some dummy query must be specified - just any string - to satisfy the option parser. If you define none of both, the whole table will be returned.\n"
                + "Use -f to provide a file with document IDs to return.\n"
                + "Use -d to display delimiters between the results.\n"
                + "Use -z to specify the target table. If the table is a subset, only documents in this subset will be returned.\n"
                + "Use -l to set a limit of returned documents.\n"
                + "Use -x to specify an XPath expression to extract specific parts of the queried XML documents.\n"
                + "Use -out to save the query results to file.", "your query"));

        modes.addOption(buildOption("h", "help", "Displays all possible parameters."));
        modes.addOption(buildOption("vn", "version", "Prints the program version."));
        modes.addOption(buildOption("t", "tables", "Displays all tables in the active scheme."));

        modes.addOption(buildOption("td", "tabledefinition", "Displays the columns of a table.", "the table"));

        modes.addOption(buildOption("ds", "displayscheme", "Displays the active scheme."));
        modes.addOption(buildOption("ch", "check",
                "Checks if a table confirms to its definition (for subsets: only primary keys!)", "table"));
        modes.addOption(buildOption("dc", "defaultconfig", "Prints the defaultConfiguration."));
        modes.addOption(buildOption("dt", "droptable", "Drops the given table.", "table"));

        modes.addOption(buildOption("lts", "listtableschemas",
                "Displays all table schema names in the configuration. The showed name index can be used as value for the -ts option."));

        modes.addOption(buildOption("dr", "delete-rows", "Deletes rows from tables whose IDs are in the delivered file in the specified table.\n" +
                "Use -z to specify the target table.", "ID file"));

        modes.addOption(buildOption("ux", "update-xmi", "Updates the XMI data of a single column given a file that adheres to a specific XML format.\n" +
                "To create a file with that format, use the query mode (-q) on an XMI column and output with the -out and -pas parameters.\n" +
                "Use the -z parameter to specify the XMI data table to update.\n" +
                "Use the -f parameter to specify the input file that adheres to the required input format.", "XMI data column name"));

        modes.setRequired(true);

        options.addOptionGroup(modes);
    }

    private static Option buildOption(String shortName, String longName, String description, String... arguments) {
        OptionBuilder.withLongOpt(longName);
        OptionBuilder.withDescription(description);
        OptionBuilder.hasArgs(arguments.length);
        for (String argument : arguments)
            OptionBuilder.withArgName(argument);
        return OptionBuilder.create(shortName);
    }
}
