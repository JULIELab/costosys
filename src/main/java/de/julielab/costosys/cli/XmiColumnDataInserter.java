package de.julielab.costosys.cli;

import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.xml.JulieXMLConstants;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class XmiColumnDataInserter {
    private final static Logger log = LoggerFactory.getLogger(XmiColumnDataInserter.class);
    private static final int BATCH_SIZE = 50;
    private static final String PMA_START = "<PubmedArticle>";
    private static final String PMA_END = "</PubmedArticle>";
    private static final String CSTS_PK_START = "<CoStoSysPrimaryKey>";
    private static final String CSTS_PK_END = "</CoStoSysPrimaryKey>";
    private static final Pattern PK_PATTERN = Pattern.compile(CSTS_PK_START + "([^<]+)" + CSTS_PK_END);
    private static final Pattern XMI_DATA_TAG_PATTERN = Pattern.compile("</?xmidata>");

    public void insertXmiColumnData(Path xmiColumnData, String superTable, String columnName, DataBaseConnector dbc) throws Exception {
        try (final CoStoSysConnection costoConn = dbc.obtainOrReserveConnection()) {
            costoConn.setAutoCommit(false);
            int processedDocuments = 0;
            try (final BufferedReader bw = FileUtilities.getReaderFromFile(xmiColumnData.toFile())) {
                final boolean isXmlField = dbc.getActiveTableFieldConfiguration().getField(columnName).get(JulieXMLConstants.TYPE).equals("xml");
                final String pkFieldName = dbc.getActiveTableFieldConfiguration().getPrimaryKeyString();
                String updateSql = "UPDATE " + superTable + " SET " + columnName + "=" + (isXmlField ? "XMLPARSE(CONTENT ?)" : "?") + " WHERE " + pkFieldName + "=?";
                final PreparedStatement ps = costoConn.prepareStatement(updateSql);
                final boolean doGzip = Boolean.parseBoolean(dbc.getActiveTableFieldConfiguration().getField(columnName).get(JulieXMLConstants.GZIP));
                String line;
                StringBuffer currentDocument = new StringBuffer();
                boolean inArticle = false;
                int currentBatchSize = 0;
                int linesRead = 0;
                while ((line = bw.readLine()) != null) {
                    ++linesRead;
                    if (line.equals(PMA_START)) {
                        inArticle = true;
                        continue;
                    } else if (line.equals(PMA_END)) {
                        addCurrentDocumentToBatch(currentDocument, ps, doGzip);
                        currentDocument.setLength(0);
                        ++currentBatchSize;
                        if (currentBatchSize == BATCH_SIZE) {
                            ps.executeBatch();
                            currentBatchSize = 0;
                        }
                        inArticle = false;
                        ++processedDocuments;
                        continue;
                    }
                    if (inArticle) {
                        currentDocument.append(line);
                    }
                    if (linesRead % 1000000 == 0)
                        log.debug("Read {} lines from the input file.", linesRead);
                }
                if (currentBatchSize > 0)
                    ps.executeBatch();
            }
            costoConn.commit();
            log.info("Updated XMI data for {} documents.", processedDocuments);
        }
    }


    private void addCurrentDocumentToBatch(StringBuffer currentDocument, PreparedStatement ps, boolean doGzip) throws Exception {
        final String docString = currentDocument.toString();
        final Matcher pkMatcher = PK_PATTERN.matcher(docString);
        if (!pkMatcher.find())
            throw new IllegalArgumentException("Input data does not match the required input format. The document ID - that should be enclosed in the tags '" + CSTS_PK_START + "' and '" + CSTS_PK_END + "' - could not be found.");
        String docId = pkMatcher.group(1);
        final String xmiDataWithoutPKElement = pkMatcher.replaceFirst("");
        final String pureXmiData = XMI_DATA_TAG_PATTERN.matcher(xmiDataWithoutPKElement).replaceAll("");
        Object columnDataToInsert;
        if (doGzip) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(pureXmiData.getBytes(StandardCharsets.UTF_8));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
                IOUtils.copy(bais, gos);
            }
            columnDataToInsert = baos.toByteArray();
        } else {
            columnDataToInsert = pureXmiData;
        }
        ps.setObject(1, columnDataToInsert);
        ps.setString(2, docId);
        ps.addBatch();
    }
}
