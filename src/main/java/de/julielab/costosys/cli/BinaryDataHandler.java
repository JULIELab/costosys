package de.julielab.costosys.cli;

import de.julielab.costosys.configuration.FieldConfig;
import de.julielab.costosys.dbconnection.CoStoSysConnection;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import de.julielab.costosys.dbconnection.util.CoStoSysException;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.XmiSplitConstants;
import de.julielab.xml.XmiSplitter;
import de.julielab.xml.binary.BinaryDecodingResult;
import de.julielab.xml.binary.BinaryJeDISNodeDecoder;
import de.julielab.xml.binary.BinaryXmiBuilder;
import de.julielab.xml.util.XMIBuilderException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.IntStream;

public class BinaryDataHandler {
    private final static Logger log = LoggerFactory.getLogger(BinaryDataHandler.class);
    private final Map<Integer, String> reverseBinaryMapping;
    private final Map<String, Boolean> featuresToMapBinary;
    private DataBaseConnector dbc;
    private String xmiMetaSchema;
    private Set<String> annotationColumnsToLoad;
    private TypeSystem typeSystem;

    /**
     *
     * @param dbc A DatabaseConnector instance.
     * @param xmiMetaSchema The Postgres schema in which the XMI namespace table is located.
     * @param annotationColumnsToLoad The names of XMI table columns where annotation modules are stored and should be retrieved.
     * @param typeSystemFiles UIMA type system descriptor files that hold the types contained in the loaded data.
     * @throws CoStoSysException If the type systems cannot be loaded.
     */
    public BinaryDataHandler(DataBaseConnector dbc, String xmiMetaSchema, Set<String> annotationColumnsToLoad, List<Path> typeSystemFiles) throws CoStoSysException {
        try {
            this.dbc = dbc;
            this.xmiMetaSchema = xmiMetaSchema;
            this.annotationColumnsToLoad = annotationColumnsToLoad;
            this.typeSystem = readTypeSystem(typeSystemFiles);
            this.reverseBinaryMapping = getReverseBinaryMappingFromDb();
            this.featuresToMapBinary = getFeaturesToMapBinaryFromDb();
        } catch (UIMAException e) {
            throw new CoStoSysException(e);
        }
    }

    private TypeSystem readTypeSystem(List<Path> typeSystemFiles) throws UIMAException {
        List<File> concreteFiles = new ArrayList<>();
        for (Path p : typeSystemFiles) {
            File f = p.toFile();
            if (f.isDirectory())
                Arrays.stream(f.listFiles((file, name) -> name.endsWith("xml"))).forEach(concreteFiles::add);
            else
                concreteFiles.add(f);
        }
        TypeSystemDescription tsDesc = TypeSystemDescriptionFactory.createTypeSystemDescription(concreteFiles.stream().map(File::getAbsolutePath).toArray(String[]::new));
        return JCasFactory.createJCas(tsDesc).getTypeSystem();
    }

    public String decodeBinaryXmiData(byte[][] data) throws CoStoSysException {
        final FieldConfig fieldConfig = dbc.getActiveTableFieldConfiguration();
        final int pkLength = fieldConfig.getPrimaryKey().length;
        LinkedHashMap<String, InputStream> xmiData = new LinkedHashMap<>();

        final int[] xmiColumnIndices = IntStream.range(pkLength, fieldConfig.getColumnsToRetrieve().length)
                .filter(i -> !fieldConfig.getColumnsToRetrieve()[i].equals("max_xmi_id") && !fieldConfig.getColumnsToRetrieve()[i].equals("sofa_mapping"))
                .toArray();
        for (int i : xmiColumnIndices) {
            if (data[i] != null) {
                String columnName = fieldConfig.getFields().get(i).get(JulieXMLConstants.NAME);
                if (columnName.equals(XmiSplitConstants.BASE_DOC_COLUMN))
                    columnName = XmiSplitter.DOCUMENT_MODULE_LABEL;
                xmiData.put(columnName, new ByteArrayInputStream(data[i]));
            }
        }

        BinaryXmiBuilder binaryBuilder = new BinaryXmiBuilder(getNamespaceMap());
        BinaryJeDISNodeDecoder binaryJeDISNodeDecoder = new BinaryJeDISNodeDecoder(annotationColumnsToLoad, true);
        try {
            final BinaryDecodingResult decodingResult = binaryJeDISNodeDecoder.decode(xmiData, typeSystem, reverseBinaryMapping, featuresToMapBinary, binaryBuilder.getNamespaces());
            ByteArrayOutputStream baos = binaryBuilder.buildXmi(decodingResult);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (IOException | XMIBuilderException e) {
            throw new CoStoSysException(e);
        }
    }

    /**
     * <p>Reads the UIMA XMI namespaces from the respective storage table in the database.</p>
     *
     * @return The map from namespace prefix to the namespace URI.
     */
    public Map<String, String> getNamespaceMap() {
        Map<String, String> map = null;
        final String nsTable = xmiMetaSchema + "." + XmiSplitConstants.XMI_NS_TABLE;
        if (dbc.tableExists(nsTable)) {
            log.debug("Reading XMI namespaces from {}", nsTable);
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.PREFIX, XmiSplitConstants.NS_URI,
                        nsTable);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getString(1), rs.getString(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            throw new IllegalStateException("The table " + nsTable + " does not exist. This is an error since it is required to re-build the XMI data. Is '" + xmiMetaSchema + "' the correct Postgres schema for the table?");
        }
        log.debug("Got XMI namespace map from table {}: {}", nsTable, map);
        if (map.isEmpty())
            throw new IllegalStateException("The table " + nsTable + " is empty. This is an error since it is required to re-build the XMI data. Is '" + xmiMetaSchema + "' the correct Postgres schema for the table?");
        return map;
    }

    private Map<String, Boolean> getFeaturesToMapBinaryFromDb() {
        Map<String, Boolean> map = null;
        final String mappingTableName = xmiMetaSchema + "." + XmiSplitConstants.BINARY_FEATURES_TO_MAP_TABLE;
        if (dbc.tableExists(mappingTableName)) {
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.BINARY_FEATURES_TO_MAP_COL_FEATURE, XmiSplitConstants.BINARY_FEATURES_TO_MAP_COL_MAP,
                        mappingTableName);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getString(1), rs.getBoolean(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            log.warn(
                    "JeDIS XMI annotation module meta table \"{}\" was not found. It is assumed that the table from which is read contains complete XMI documents.",
                    xmiMetaSchema + "." + XmiSplitConstants.BINARY_FEATURES_TO_MAP_TABLE);
        }
        return map;
    }

    public Map<Integer, String> getReverseBinaryMappingFromDb() {
        Map<Integer, String> map = null;
        final String mappingTableName = xmiMetaSchema + "." + XmiSplitConstants.BINARY_MAPPING_TABLE;
        if (dbc.tableExists(mappingTableName)) {
            try (CoStoSysConnection conn = dbc.obtainOrReserveConnection(true)) {
                map = new HashMap<>();
                Statement stmt = conn.createStatement();
                String sql = String.format("SELECT %s,%s FROM %s", XmiSplitConstants.BINARY_MAPPING_COL_ID, XmiSplitConstants.BINARY_MAPPING_COL_STRING,
                        mappingTableName);
                ResultSet rs = stmt.executeQuery(String.format(sql));
                while (rs.next())
                    map.put(rs.getInt(1), rs.getString(2));
            } catch (SQLException e) {
                e.printStackTrace();
                SQLException ne = e.getNextException();
                if (null != ne)
                    ne.printStackTrace();
            }
        } else {
            log.warn(
                    "JeDIS XMI annotation module meta table \"{}\" was not found. It is assumed that the table from which is read contains complete XMI documents.",
                    xmiMetaSchema + "." + XmiSplitConstants.BINARY_MAPPING_TABLE);
        }
        return map;
    }
}
