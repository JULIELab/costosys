package de.julielab.costosys.medline;

import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import de.julielab.costosys.Constants;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Updater  {

	private final static Logger log = LoggerFactory.getLogger(Updater.class);

	/**
	 * Name of the table that keeps track of already imported and finished Medline
	 * update files. Value: {@value #UPDATE_TABLE}.
	 */
	public static final String UPDATE_TABLE = Constants.DEFAULT_DATA_SCHEMA + "._medline_update_files";

	public static final String COLUMN_FILENAME = "update_file_name";
	public static final String COLUMN_IS_IMPORTED = "is_imported";
	public static final String COLUMN_TIMESTAMP = "timestamp_of_import";

	private final String medlineFile;

	private ServiceLoader<IDocumentDeleter> documentDeleterLoader;

	private HierarchicalConfiguration<ImmutableNode> configuration;

	public Updater(HierarchicalConfiguration<ImmutableNode> configuration) {
		this.configuration = configuration;
		this.medlineFile = configuration.getString(ConfigurationConstants.UPDATE_INPUT);
		documentDeleterLoader = ServiceLoader.load(IDocumentDeleter.class);
	}

	public void process(DataBaseConnector dbc) throws MedlineUpdateException, IOException {
		log.info("Updating from {} into database at {}", medlineFile, dbc.getDbURL());
        File[] updateFiles = getMedlineFiles(medlineFile);
        if (updateFiles != null && updateFiles.length > 0) {
			List<File> unprocessedMedlineUpdates = getUnprocessedMedlineUpdates(updateFiles, dbc);
			configureDocumentDeleters();
			// Get the names of all deleters to be applied.
			String[] configuredDeleters = configuration.getStringArray(ConfigurationConstants.DELETER_NAME);
			for (File file : unprocessedMedlineUpdates) {
				log.info("Processing file {}.", file.getAbsoluteFile());
				dbc.updateFromXML(file.getAbsolutePath(), Constants.DEFAULT_DATA_TABLE_NAME);
				List<String> pmidsToDelete = getPmidsToDelete(file);
				for (Iterator<IDocumentDeleter> it = documentDeleterLoader.iterator(); it.hasNext(); ) {
					IDocumentDeleter documentDeleter = it.next();
					if (!documentDeleter.hasName(configuredDeleters))
						continue;
					// This is very hacky. Might work for now, does not scale, of course.
					if (documentDeleter instanceof MedlineDataTableDocumentDeleter)
						((MedlineDataTableDocumentDeleter) documentDeleter).setDbc(dbc);
					documentDeleter.deleteDocuments(pmidsToDelete);
				}

				// As last thing, mark the current update file as finished.
				markFileAsImported(file, dbc);
			}
		}
	}

    protected File[] getMedlineFiles(String medlinePathString) throws FileNotFoundException {
        File medlinePath = new File(medlinePathString);
        if (!medlinePath.exists())
            throw new FileNotFoundException("File \"" + medlinePathString + "\" was not found.");
        if (!medlinePath.isDirectory())
            return new File[] { medlinePath };
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

	private void configureDocumentDeleters() throws MedlineDocumentDeletionException {
		if (configuration.containsKey(ConfigurationConstants.DELETION)) {
			// Get all deletion configurations.
			List<HierarchicalConfiguration<ImmutableNode>> deletionConfigs = configuration
					.configurationsAt(ConfigurationConstants.DELETION);
			// For each deletion configuration, search for the deleter specified in the
			// respective configuration and set the deletion sub configuration.
			for (HierarchicalConfiguration<ImmutableNode> deletionConf : deletionConfigs) {
				for (Iterator<IDocumentDeleter> it = documentDeleterLoader.iterator(); it.hasNext();) {
					IDocumentDeleter documentDeleter = it.next();
					if (documentDeleter.hasName(deletionConf.getString(ConfigurationConstants.DELETER_NAME)))
						documentDeleter.configure(deletionConf);
				}
			}
		}
	}

	private static List<File> getUnprocessedMedlineUpdates(File[] updateFiles, DataBaseConnector dbc) {
            List<File> unprocessedFiles = new ArrayList<>();
		try (CoStoSysConnection coStoSysConnection = dbc.obtainOrReserveConnection()) {

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
                // And now a last but very important step: Sort the list to be in
                // the correct update-sequence. If we don't, it can happen that we
                // process a newer update before an older which will then result in
                // deprecated documents kept in the database.
                Collections.sort(unprocessedFiles, new Comparator<File>() {

                    @Override
                    public int compare(File f1, File f2) {
                        // The files are named like "medline13n0792.xml.zip", i.e.
                        // their sequence number if part of the file name. We just
                        // have to sort by string comparison.
                        return f1.getName().compareTo(f2.getName());
                    }
                });
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
		return unprocessedFiles;
	}

	private static List<String> getPmidsToDelete(File file) {
		List<String> pmidsToDelete = new ArrayList<String>();
		String forEachXpath = "/MedlineCitationSet/DeleteCitation/PMID";
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

	/**
	 * Marks the file <code>file</code> as being imported in the database table
	 * {@link #UPDATE_TABLE}.
	 * 
	 * @param file
	 * @param dbc
	 */
	private void markFileAsImported(File file, DataBaseConnector dbc) {
        try (CoStoSysConnection coStoSysConnection = dbc.obtainOrReserveConnection()) {
            Connection conn = coStoSysConnection.getConnection();
            String sql = null;
            try {
                sql = String.format(
                        "UPDATE %s SET %s = TRUE, %s = '" + new Timestamp(System.currentTimeMillis()) + "' WHERE %s = '%s'",
                        UPDATE_TABLE, COLUMN_IS_IMPORTED, COLUMN_TIMESTAMP, COLUMN_FILENAME, file.getName());
                conn.createStatement().execute(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("SQL command was: {}", sql);
            }
        }
	}
}
