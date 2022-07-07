package de.julielab.costosys.cli;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        final ExecutorService executorService = Executors.newFixedThreadPool(6);
        try (final CoStoSysConnection costoConn = dbc.obtainOrReserveConnection()) {
            costoConn.setAutoCommit(false);
            int processedDocuments = 0;
            List<Object[]> batch = new ArrayList<>(BATCH_SIZE);
            try (final BufferedReader bw = FileUtilities.getReaderFromFile(xmiColumnData.toFile())) {
                final Map<String, String> field = dbc.getActiveTableFieldConfiguration().getField(columnName);
                if (field == null)
                    throw new IllegalArgumentException("The active table configuration does not contain a field named '" + columnName + "'.");
                final boolean isXmlField = field.get(JulieXMLConstants.TYPE).equals("xml");
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
                    if (linesRead % 1000000 == 0)
                        log.debug("Read {} lines from the input file.", linesRead);
                    if (line.equals(PMA_START)) {
                        inArticle = true;
                        continue;
                    } else if (line.equals(PMA_END)) {
                        final String docString = currentDocument.toString();
                        currentDocument.setLength(0);
                        executorService.submit(() -> {
                            try {
                                addCurrentDocumentToBatch(docString, batch, doGzip);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                        ++currentBatchSize;

                        synchronized (batch) {
                            if (batch.size() >= BATCH_SIZE) {
                                for (Object[] s : batch) {
                                    ps.setObject(1, s[0]);
                                    ps.setObject(2, s[1]);
                                    ps.addBatch();
                                }
                                batch.clear();
                                ps.executeBatch();
                                currentBatchSize = 0;
                            }
                        }
                        inArticle = false;
                        ++processedDocuments;
                        continue;
                    }
                    if (inArticle) {
                        currentDocument.append(line);
                    }
                }
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.MINUTES);
                synchronized (batch) {
                    if (batch.size() > 0) {
                        log.info("Sending last, incomplete batch with annotations for {} documents to database", batch.size());
                        for (Object[] s : batch) {
                            ps.setObject(1, s[0]);
                            ps.setObject(2, s[1]);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    }
                }
            }
            costoConn.commit();
            log.info("Updated XMI data for {} documents.", processedDocuments);
        }
    }


    private void addCurrentDocumentToBatch(String docString, List<Object[]> batch, boolean doGzip) throws Exception {
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
                copyInputStream(bais, gos);
            }
            columnDataToInsert = baos.toByteArray();
        } else {
            columnDataToInsert = pureXmiData;
        }
        synchronized (batch) {
            batch.add(new Object[]{columnDataToInsert, docId});
        }
    }

    private void copyInputStream(InputStream bais, OutputStream gos) throws IOException {
        byte[] b = new byte[4096];
        int n;
        while ((n = bais.read(b)) != -1) {
            gos.write(b, 0, n);
        }
    }
}
