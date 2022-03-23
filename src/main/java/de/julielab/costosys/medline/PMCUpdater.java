package de.julielab.costosys.medline;

import de.julielab.costosys.Constants;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.java.utilities.FileUtilities;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static de.julielab.java.utilities.ConfigurationUtilities.dot;

public class PMCUpdater {

    /**
     * Name of the table that keeps track of already imported and finished Medline
     * update files. Value: {@value #UPDATE_TABLE}.
     */
    public static final String UPDATE_TABLE = Constants.DEFAULT_DATA_SCHEMA + "._pmc_update_files";
    public static final String COLUMN_FILENAME = "update_file_name";
    public static final String COLUMN_IS_IMPORTED = "is_imported";
    public static final String COLUMN_TIMESTAMP = "timestamp_of_import";
    private final static Logger log = LoggerFactory.getLogger(PMCUpdater.class);
    private final String[] pmcFileDirectories;

    private ServiceLoader<IDocumentDeleter> documentDeleterLoader;

    private HierarchicalConfiguration<ImmutableNode> configuration;

    public PMCUpdater(HierarchicalConfiguration<ImmutableNode> configuration) {
        this.configuration = configuration;
        this.pmcFileDirectories = configuration.getStringArray(ConfigurationConstants.DIRECTORY);
        documentDeleterLoader = ServiceLoader.load(IDocumentDeleter.class);
        log.info("Initialized PMC Updater with file directories {} and document deleters {}.", this.pmcFileDirectories, configuration.getStringArray(dot(ConfigurationConstants.DELETION, ConfigurationConstants.DELETER)));
        log.debug("Loaded the following document deleters: {}", documentDeleterLoader.stream().map(d -> d.get().getName()).collect(Collectors.joining(", ")));
    }

    private static List<File> getUnprocessedPMCUpdates(File[] updateFiles, DataBaseConnector dbc) throws MedlineUpdateException {
        List<File> unprocessedFiles = new ArrayList<>();
        try (CoStoSysConnection coStoSysConnection = dbc.obtainOrReserveConnection(true)) {

            Connection conn = coStoSysConnection.getConnection();

            Set<String> updateFileNameSet = new HashSet<>();
            for (File f : updateFiles)
                updateFileNameSet.add(f.getName());

            try {
                Statement st = conn.createStatement();
                // Create the table listing the update file names, if it does not
                // exist.
                boolean exists = dbc.tableExists(UPDATE_TABLE);
                if (!exists) {

                    String createUpdateTable = String.format(
                            "CREATE TABLE %s " + "(%s TEXT PRIMARY KEY," + "%s BOOLEAN DEFAULT FALSE,"
                                    + "%s TIMESTAMP WITHOUT TIME ZONE)",
                            UPDATE_TABLE, COLUMN_FILENAME, COLUMN_IS_IMPORTED, COLUMN_TIMESTAMP);
                    st.execute(createUpdateTable);
                }
                // Determine which update files are new.
                Set<String> filenamesInDBSet = new HashSet<String>();
                ResultSet rs = st.executeQuery(String.format("SELECT %s from %s", COLUMN_FILENAME, UPDATE_TABLE));

                while (rs.next()) {
                    String filename = rs.getString(COLUMN_FILENAME);
                    filenamesInDBSet.add(filename);
                }
                // From all update files we found in the update directory, remove
                // the files already residing in the database.
                updateFileNameSet.removeAll(filenamesInDBSet);

                // Now add all new filenames.
                conn.setAutoCommit(false);
                PreparedStatement ps = conn.prepareStatement(String.format("INSERT INTO %s VALUES (?)", UPDATE_TABLE));
                for (String filename : updateFileNameSet) {
                    ps.setString(1, filename);
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);

                // Retrieve all those filenames from the database which have not yet
                // been processed. This includes the new files we just entered into
                // the database table.
                String sql = String.format("SELECT %s FROM %s WHERE %s = FALSE", COLUMN_FILENAME, UPDATE_TABLE,
                        COLUMN_IS_IMPORTED);
                rs = st.executeQuery(sql);
                final Set<String> unprocessedFileSet = new HashSet<String>();
                while (rs.next()) {
                    unprocessedFileSet.add(rs.getString(COLUMN_FILENAME));
                }
                // Create a list of files which will only contain all files
                // to be processed.
                for (File updateFile : updateFiles) {
                    if (unprocessedFileSet.contains(updateFile.getName()))
                        unprocessedFiles.add(updateFile);
                }
            } catch (SQLException e) {
                throw new MedlineUpdateException(e);
            }
        }
        return unprocessedFiles;
    }

    /**
     * Reads the filelist.txt file for the given XML archive file. Extracts the IDs that are marked as "retracted".
     * Note tha per PMC policy, retracted articles are not actually removed from PMC. So we might actually keep them, too.
     *
     * @param file
     * @return
     * @see <url>https://www.ncbi.nlm.nih.gov/labs/pmc/about/guidelines/#retract</url>
     */
    private static List<String> getPmcidsToDelete(File file) {
        File tsvFile = new File(file.getAbsolutePath().replace(".tar.gz", "filelist.txt"));
        List<String> pmcidsToDelete = new ArrayList<>();
        int pmcFilePathIndex = 0;
        int retractionIndex = 6;
        try (BufferedReader br = FileUtilities.getReaderFromFile(tsvFile)) {
            br.lines().map(l -> l.split("\\t"))
                    .filter(s -> s[retractionIndex].equalsIgnoreCase("yes"))
                    .map(s -> s[pmcFilePathIndex]).map(path -> path.substring(path.indexOf('/') + 1))
                    .map(filename -> filename.replace(".xml", ""))
                    .forEach(pmcidsToDelete::add);
        } catch (IOException e) {
            log.error("Could not process the {} file. No retracted documents will be removed.");
        }
        return pmcidsToDelete;
    }

    public void process(DataBaseConnector dbc, boolean ignoreAlreadyProcessed) throws MedlineUpdateException, IOException {
        if (ignoreAlreadyProcessed)
            log.info("Ignoring update file processing state in the database and processing all files in the directories {}.", pmcFileDirectories);
        configureDocumentDeleters();
        for (String directory : pmcFileDirectories) {
            log.info("Updating from {} into database at {}", directory, dbc.getDbURL());
            File[] pmcFiles = getPMCFiles(directory);
            if (pmcFiles != null && pmcFiles.length > 0) {
                List<File> unprocessedPmcUpdates = ignoreAlreadyProcessed ? Arrays.asList(pmcFiles) : getUnprocessedPMCUpdates(pmcFiles, dbc);
                // A very important step: Sort the list to be in
                // the correct update-sequence. If we don't, it can happen that we
                // process a newer update before an older which will then result in
                // deprecated documents kept in the database.
                // The files are named like "oa_comm_xml.PMC002xxxxxx.baseline.2022-03-04.tar.gz" for
                // baseline files and "oa_comm_xml.incr.2022-01-07.tar.gz" for update files, i.e.
                // their release date is part of the file name. First, sort the baseline files to the beginning,
                // then simply use string comparison.
                Collections.sort(unprocessedPmcUpdates, Comparator.comparing(File::getName, (n1, n2) -> {
                    String baseline = "baseline";
                    boolean n1ContainsBaseline = n1.contains(baseline);
                    boolean n2ContainsBaseline = n2.contains(baseline);
                    if (n1ContainsBaseline && ! n2ContainsBaseline)
                        return -1;
                    if (n2ContainsBaseline && !n1ContainsBaseline)
                        return 1;
                    return 0;
                }).thenComparing(File::getName));
                // Get the names of all deleters to be applied.
                String[] configuredDeleters = configuration.getStringArray(dot(ConfigurationConstants.DELETION, ConfigurationConstants.DELETER));
                for (File file : unprocessedPmcUpdates) {
                    log.info("Processing file {}.", file.getAbsoluteFile());
                    dbc.updateFromXML(file.getAbsolutePath(), Constants.DEFAULT_DATA_TABLE_NAME, true);
                    // the algorithm to retrieve retracted PMC ids is implemented. But PMC policy says that they
                    // keep those documents around. So we don't bother as well, for now.
                    List<String> pmidsToDelete = Collections.emptyList();//getPmcidsToDelete(file);
                    Set<IDocumentDeleter> appliedDeleters = new HashSet<>();
                    for (Iterator<IDocumentDeleter> it = documentDeleterLoader.iterator(); it.hasNext(); ) {
                        IDocumentDeleter documentDeleter = it.next();
                        log.debug("Checking if deleter {} is part of the deleter configuration.", documentDeleter.getName());
                        if (!documentDeleter.isOneOf(configuredDeleters)) {
                            log.debug("Skipping document deleter {}.", documentDeleter.getName());
                            continue;
                        }
                        log.debug("Applying document deleter {}.", documentDeleter.getName());
                        // This is very hacky. Might work for now, does not scale well, of course.
                        if (documentDeleter instanceof SimplePKDataTableDocumentDeleter)
                            ((SimplePKDataTableDocumentDeleter) documentDeleter).setDbc(dbc);
                        documentDeleter.deleteDocuments(pmidsToDelete);
                        appliedDeleters.add(documentDeleter);
                    }
                    if (appliedDeleters.size() < configuredDeleters.length)
                        throw new IllegalStateException("Not all document deleters could be applied. Configured deleters were " + Arrays.toString(configuredDeleters) + " but applied were only " + (appliedDeleters.isEmpty() ? "none" : appliedDeleters.stream().map(IDocumentDeleter::getName).collect(Collectors.joining(", "))));

                    // As last thing, mark the current update file as finished.
                    markFileAsImported(file, dbc);
                }
            }
        }
    }

    protected File[] getPMCFiles(String pathString) throws IOException {
        File pmcPath = new File(pathString);
        if (!pmcPath.exists())
            throw new FileNotFoundException("File \"" + pathString + "\" was not found.");
        if (!pmcPath.isDirectory())
            return new File[]{pmcPath};
        File[] pmcFiles = Files.walk(Path.of(pathString), FileVisitOption.FOLLOW_LINKS).filter(p -> p.toString().endsWith("gz") || p.toString().endsWith("gzip") || p.toString().endsWith("zip") || p.toString().endsWith("tgz") || p.toString().endsWith("tar.gz")).map(Path::toFile).toArray(File[]::new);
        // Check whether anything has been read.
        if (pmcFiles == null || pmcFiles.length == 0) {
            log.info("No files in ZIP, GZIP or TGZ format found in directory {}. No update will be performed.", pathString);
        }
        return pmcFiles;
    }

    private void configureDocumentDeleters() throws DocumentDeletionException {
        log.trace("Configuring document deleters");
        // We cannot test for the DELETION element because there is no value bound to it; it is only part of
        // the path to longer configuration keys.
        if (configuration.containsKey(dot(ConfigurationConstants.DELETION, ConfigurationConstants.DELETER))) {
            // Get all deletion configurations.
            List<HierarchicalConfiguration<ImmutableNode>> deletionConfigs = configuration
                    .configurationsAt(ConfigurationConstants.DELETION);
            log.trace("Found {} deleter configurations.", deletionConfigs.size());
            // For each deletion configuration, search for the deleter specified in the
            // respective configuration and set the deletion sub configuration.
            for (HierarchicalConfiguration<ImmutableNode> deletionConf : deletionConfigs) {
                for (Iterator<IDocumentDeleter> it = documentDeleterLoader.iterator(); it.hasNext(); ) {
                    IDocumentDeleter documentDeleter = it.next();
                    log.trace("Configuring deleter {}", documentDeleter.getName());
                    if (documentDeleter.isOneOf(deletionConf.getString(ConfigurationConstants.DELETER)))
                        documentDeleter.configure(deletionConf);
                    else
                        log.trace("Skipping the configuration of {} because it is not specified in the configuration.", documentDeleter.getName());
                }
            }
        } else {
            log.trace("No document deleters were specified in the configuration.");
        }
    }

    /**
     * Marks the file <code>file</code> as being imported in the database table
     * {@link #UPDATE_TABLE}.
     *
     * @param file
     * @param dbc
     */
    private void markFileAsImported(File file, DataBaseConnector dbc) throws MedlineUpdateException {
        try (CoStoSysConnection coStoSysConnection = dbc.obtainOrReserveConnection(true)) {
            Connection conn = coStoSysConnection.getConnection();
            String sql = null;
            try {
                log.debug("Marking update file {} as imported.", file);
                sql = String.format(
                        "UPDATE %s SET %s = TRUE, %s = '" + new Timestamp(System.currentTimeMillis()) + "' WHERE %s = '%s'",
                        UPDATE_TABLE, COLUMN_IS_IMPORTED, COLUMN_TIMESTAMP, COLUMN_FILENAME, file.getName());
                conn.createStatement().execute(sql);
                conn.commit();
            } catch (SQLException e) {
                log.error("SQL command was: {}", sql);
                throw new MedlineUpdateException(e);
            }
        }
    }
}
