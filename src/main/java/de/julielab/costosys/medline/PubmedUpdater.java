package de.julielab.costosys.medline;

import de.julielab.costosys.Constants;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static de.julielab.java.utilities.ConfigurationUtilities.dot;

public class PubmedUpdater {

    /**
     * Name of the table that keeps track of already imported and finished Medline
     * update files. Value: {@value #UPDATE_TABLE}.
     */
    public static final String UPDATE_TABLE = Constants.DEFAULT_DATA_SCHEMA + "._medline_update_files";
    public static final String COLUMN_FILENAME = "update_file_name";
    public static final String COLUMN_IS_IMPORTED = "is_imported";
    public static final String COLUMN_TIMESTAMP = "timestamp_of_import";
    private final static Logger log = LoggerFactory.getLogger(PubmedUpdater.class);
    private final String[] medlineFileDirectories;

    private ServiceLoader<IDocumentDeleter> documentDeleterLoader;

    private HierarchicalConfiguration<ImmutableNode> configuration;

    public PubmedUpdater(HierarchicalConfiguration<ImmutableNode> configuration) {
        this.configuration = configuration;
        this.medlineFileDirectories = configuration.getStringArray(ConfigurationConstants.DIRECTORY);
        documentDeleterLoader = ServiceLoader.load(IDocumentDeleter.class);
        log.info("Initialized MEDLINE Updater with file directories {} and document deleters {}.", this.medlineFileDirectories, configuration.getStringArray(dot(ConfigurationConstants.DELETION, ConfigurationConstants.DELETER)));
        log.debug("Loaded the following document deleters: {}", documentDeleterLoader.stream().map(d -> d.get().getName()).collect(Collectors.joining(", ")));
    }

    private static List<File> getUnprocessedMedlineUpdates(File[] updateFiles, DataBaseConnector dbc) throws MedlineUpdateException {
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

    private static List<String> getPmidsToDelete(File file) {
        List<String> pmidsToDelete = new ArrayList<>();
        String forEachXpath = "/PubmedArticleSet/DeleteCitation/PMID";
        List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
        Map<String, String> field = new HashMap<String, String>();
        field.put(JulieXMLConstants.NAME, Constants.PMID_FIELD_NAME);
        field.put(JulieXMLConstants.XPATH, ".");
        fields.add(field);

        int bufferSize = 1000;
        Iterator<Map<String, Object>> it = JulieXMLTools.constructRowIterator(file.getAbsolutePath(), bufferSize,
                forEachXpath, fields, false);

        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            String pmid = (String) row.get(Constants.PMID_FIELD_NAME);
            pmidsToDelete.add(pmid);
        }
        return pmidsToDelete;
    }

    public void process(DataBaseConnector dbc, boolean ignoreAlreadyProcessed) throws MedlineUpdateException, IOException {
        if (ignoreAlreadyProcessed)
            log.info("Ignoring update file processing state in the database and processing all files in the directories {}.", medlineFileDirectories);
        configureDocumentDeleters();
        for (String directory : medlineFileDirectories) {
            log.info("Updating from {} into database at {}", directory, dbc.getDbURL());
            File[] medlineFiles = getMedlineFiles(directory);
            if (medlineFiles != null && medlineFiles.length > 0) {
                List<File> unprocessedMedlineUpdates = ignoreAlreadyProcessed ? Arrays.asList(medlineFiles) : getUnprocessedMedlineUpdates(medlineFiles, dbc);
                // A very important step: Sort the list to be in
                // the correct update-sequence. If we don't, it can happen that we
                // process a newer update before an older which will then result in
                // deprecated documents kept in the database.
                // The files are named like "medline13n0792.xml.zip", i.e.
                // their sequence number is part of the file name. We just
                // have to sort by string comparison.
                Collections.sort(unprocessedMedlineUpdates, Comparator.comparing(File::getName));
                // Get the names of all deleters to be applied.
                String[] configuredDeleters = configuration.getStringArray(dot(ConfigurationConstants.DELETION, ConfigurationConstants.DELETER));
                for (File file : unprocessedMedlineUpdates) {
                    log.info("Processing file {}.", file.getAbsoluteFile());
                    dbc.updateFromXML(file.getAbsolutePath(), Constants.DEFAULT_DATA_TABLE_NAME, true);
                    List<String> pmidsToDelete = getPmidsToDelete(file);
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

    protected File[] getMedlineFiles(String medlinePathString) throws FileNotFoundException {
        File medlinePath = new File(medlinePathString);
        if (!medlinePath.exists())
            throw new FileNotFoundException("File \"" + medlinePathString + "\" was not found.");
        if (!medlinePath.isDirectory())
            return new File[]{medlinePath};
        File[] medlineFiles = medlinePath.listFiles(file -> {
            String filename = file.getName();
            return filename.endsWith("gz") || filename.endsWith("gzip") || filename.endsWith("zip");
        });
        // Check whether anything has been read.
        if (medlineFiles == null || medlineFiles.length == 0) {
            log.info("No (g)zipped files found in directory {}. No update will be performed.", medlinePathString);
        }
        return medlineFiles;
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
            } catch (SQLException e) {
                log.error("SQL command was: {}", sql);
                throw new MedlineUpdateException(e);
            }
        }
    }
}
