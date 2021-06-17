/**
 * FieldDefinition.java
 * <p>
 * Copyright (c) 2011, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 * <p>
 * Author: faessler
 * <p>
 * Current version: 1.0
 * Since version:   1.0
 * <p>
 * Creation date: 11.03.2011
 **/

package de.julielab.costosys.configuration;

import com.ximpleware.*;
import de.julielab.costosys.Constants;
import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

/**
 * This class holds the definition of fields for the database table to work
 * with. The definition was read from the configuration XML file.
 *
 * @author faessler
 */
public class FieldConfig extends ConfigBase {

    private static final String XPATH_CONF_SCHEMA_INFO = "//DBSchemaInformation";
    private static final String XPATH_CONF_SCHEMES = XPATH_CONF_SCHEMA_INFO + "/tableSchemas";
    private static final String XPATH_CONF_SCHEME = XPATH_CONF_SCHEMES + "/tableSchema";

    private static final String XPATH_CONF_FIELD_TEMPLATE = XPATH_CONF_SCHEME
            + "[@name='%s']/field";
    private static final String XPATH_CONF_ACTIVE_SCHEME_TEMPLATE = XPATH_CONF_SCHEME
            + "[@name='%s']";
    private final String name;
    private List<Map<String, String>> fields;
    private Map<String, Map<String, String>> fieldNameMap;
    private String forEachXPath;
    private boolean binary;
    private String[] primaryKey;
    private String[] columns;
    private String timestampFieldName = null;
    private List<Integer> primaryKeyFieldNumbers = null;
    private byte[] configData;

    public FieldConfig(byte[] configData, String schemaName) throws VTDException {
        this.configData = configData;
        this.name = schemaName;
        buildFields(configData, schemaName);
    }

    public FieldConfig(List<Map<String, String>> fields, String forEachXPath, String schemaName) {
        this.forEachXPath = forEachXPath;
        this.name = schemaName;
        this.fields = fields;
        fieldNameMap = new HashMap<>();
        for (Map<String, String> field : fields) {
            String name = field.get(JulieXMLConstants.NAME);
            if (name == null)
                throw new IllegalArgumentException("The passed field configuration contains the field \"" + field + "\" " +
                        "that does specify the required \"" + JulieXMLConstants.NAME + "\" property");
            if (field.get(JulieXMLConstants.TYPE) == null)
                throw new IllegalArgumentException("The passed field configuration contains the field \"" + field + "\" " +
                        "that does specify the required \"" + JulieXMLConstants.TYPE + "\" property");
            fieldNameMap.put(name, field);
            if (field.get(JulieXMLConstants.TIMESTAMP) != null && Boolean.parseBoolean(field.get(JulieXMLConstants.TIMESTAMP)))
                timestampFieldName = name;
        }
        primaryKey = fields.stream().
                filter(field -> Boolean.parseBoolean((field.get(JulieXMLConstants.PRIMARY_KEY)))).
                map(field -> field.get(JulieXMLConstants.NAME)).
                toArray(String[]::new);
        columns = fields.stream().
                map(field -> field.get(JulieXMLConstants.NAME)).
                toArray(String[]::new);
        primaryKeyFieldNumbers = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            Map<String, String> field = fields.get(i);
            if (Boolean.parseBoolean((field.get(JulieXMLConstants.PRIMARY_KEY))))
                primaryKeyFieldNumbers.add(i);
        }
    }

    public static boolean isKnownType(String type) {
        return isStringType(type) || isTimestampWithoutTZType(type) || isStringTypeArray(type)
                || isIntegerType(type) || isBooleanType(type) || isBinaryDataType(type) || isXmlType(type);
    }

    public static boolean isBinaryDataType(String type) {
        return type.equals(Constants.TYPE_BINARY_DATA);
    }

    /**
     * @param type
     * @return
     */
    public static boolean isStringTypeArray(String type) {
        return type.equals(Constants.TYPE_TEXT_ARRAY) || type.equals(Constants.TYPE_VARCHAR_ARRAY);
    }

    public static boolean isXmlType(String type) {
        return type.equals(Constants.TYPE_XML);
    }

    /**
     * @param type
     * @return
     */
    public static boolean isBooleanType(String type) {
        return type.equals(Constants.TYPE_BOOLEAN);
    }

    /**
     * Returns true if the string <code>type</code> equals the SQL
     * "timestamp without time zone" type.
     *
     * @param type String to test.
     * @return True if <code>type</code> denotes a timestamp type without
     * timezone information.
     */
    public static boolean isTimestampWithoutTZType(String type) {
        return type.equals(Constants.TYPE_TIMESTAMP_WITHOUT_TIMEZONE);
    }

    public static boolean isStringType(String type) {
        return type.equals(Constants.TYPE_TEXT);
    }

    /**
     * @param type
     * @return
     */
    public static boolean isIntegerType(String type) {
        return type.equals(Constants.TYPE_INTEGER);
    }

    public static Map<String, String> createField(String... configuration) {
        if (configuration.length % 2 == 1)
            throw new IllegalArgumentException("An even number of arguments is required. The even indexes " +
                    "are field property keys, the odd indexes are the values to the previous key.");
        Map<String, String> field = new HashMap<>();
        for (int i = 0; i < configuration.length; i = i + 2) {
            String s = configuration[i];
            field.put(s, configuration[i + 1]);
        }
        return field;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public boolean isBinary() {
        return binary;
    }

    private void buildFields(byte[] mergedConfData, String activeSchemeName)
            throws ParseException, XPathParseException, XPathEvalException, NavException {
        VTDGen vg = new VTDGen();
        vg.setDoc(mergedConfData);
        vg.parse(true);
        VTDNav vn = vg.getNav();
        AutoPilot ap = new AutoPilot(vn);

        ap.selectXPath(String.format(XPATH_CONF_ACTIVE_SCHEME_TEMPLATE, activeSchemeName));
        if (ap.evalXPath() != -1) {
            int attrIndex = vn.getAttrVal(JulieXMLConstants.FOR_EACH);
            if (attrIndex != -1)
                forEachXPath = vn.toString(attrIndex);
            int attrIndexB = vn.getAttrVal(JulieXMLConstants.BINARY);
            if (attrIndexB != -1)
                binary = Boolean.parseBoolean(vn.toString(attrIndexB));
        }

        ap.selectXPath(String.format(XPATH_CONF_FIELD_TEMPLATE, activeSchemeName));
        AutoPilot ap2 = new AutoPilot(vn);
        fields = new ArrayList<>();
        fieldNameMap = new HashMap<>();

        boolean xPathFound = false;
        while (ap.evalXPath() != -1) {
            xPathFound = true;
            Map<String, String> field = new LinkedHashMap<>();
            String fieldName = null;
            int i = -1;
            ap2.selectAttr("*");
            while ((i = ap2.iterateAttr()) != -1) {
                String attrName = vn.toString(i);
                String attrValue = vn.toRawString(i + 1);
                // I actually don't know if there still are unsupported types since in the DataBaseConnector we just set objects
                if (attrName.equals(JulieXMLConstants.TYPE))
                    if (!isKnownType(attrValue))
                        throw new IllegalArgumentException("Type \"" + attrValue
                                + "\" is not supported by this tool.");
                field.put(attrName, attrValue);
                if (attrName.equals(JulieXMLConstants.NAME))
                    fieldName = attrValue;
                if (attrName.equals(JulieXMLConstants.TIMESTAMP) && Boolean.parseBoolean(attrValue))
                    timestampFieldName = fieldName;
            }
            // These fields potentially still contain field
            // definitions which do not rely on the XML document
            // itself but e.g. on the file name. These fields
            // are treated in the "prepare" method.
            fields.add(field);
            fieldNameMap.put(fieldName, field);
        }
        if (!xPathFound)
            throw new TableSchemaDoesNotExistException("No field schema with name \""
                    + activeSchemeName + "\" was found.");
    }

    public List<Map<String, String>> getFields() {
        return fields;
    }

    public Map<String, String> getField(String fieldName) {
        return fieldNameMap.get(fieldName);
    }

    public String getForEachXPath() {
        return forEachXPath;
    }

    public void setForEachXPath(String forEachXPath) {
        this.forEachXPath = forEachXPath;
    }

    public boolean isOfTimestampWithoutTZType(String fieldName) {
        return isTimestampWithoutTZType(fieldNameMap.get(fieldName).get(JulieXMLConstants.TYPE));
    }

    public boolean isOfStringType(String fieldName) {
        return isStringType(fieldNameMap.get(fieldName).get(JulieXMLConstants.TYPE));
    }

    public boolean isOfStringType(Map<String, String> field) {
        return isStringType(field.get(JulieXMLConstants.TYPE));
    }

    public boolean isOfIntegerType(Map<String, String> field) {
        return isIntegerType(field.get(JulieXMLConstants.TYPE));
    }

    public boolean isOfBinaryDataType(Map<String, String> field) {
        return isBinaryDataType(field.get(JulieXMLConstants.TYPE));
    }

    /**
     * @return - An Array with the names off all primary keys
     */
    public String[] getPrimaryKey() {
        if (primaryKey == null) {
            primaryKey = getPrimaryKeyFields().
                    map(field -> field.get(JulieXMLConstants.NAME)).
                    toArray(String[]::new);
        }
        return primaryKey;
    }

    public Stream<Map<String, String>> getPrimaryKeyFields() {
        return fields.stream().filter(field -> Boolean.parseBoolean(field.get(JulieXMLConstants.PRIMARY_KEY)));
    }

    /**
     * @return - The indices of those fields which are primary keys, beginning
     * with 0
     */
    public List<Integer> getPrimaryKeyFieldNumbers() {
        if (primaryKeyFieldNumbers == null) {
            List<Integer> fieldNumbers = new ArrayList<>();
            int i = 0;
            for (Map<String, String> field : fields) {
                if (Boolean.parseBoolean(field.get(JulieXMLConstants.PRIMARY_KEY)))
                    fieldNumbers.add(i);
                ++i;
            }
            primaryKeyFieldNumbers = fieldNumbers;
        }
        return primaryKeyFieldNumbers;
    }

    public String[] getColumnsToRetrieve() {
        List<String> retrieveColumnNames = new ArrayList<>();
        for (Map<String, String> field : fields) {
            if (Boolean.parseBoolean(field.get(JulieXMLConstants.RETRIEVE)))
                retrieveColumnNames.add(field.get(JulieXMLConstants.NAME));
        }
        String[] columnsToRetrieve = new String[retrieveColumnNames.size()];
        retrieveColumnNames.toArray(columnsToRetrieve);
        return columnsToRetrieve;
    }

    public List<Map<String, String>> getFieldsToRetrieve() {
        List<Map<String, String>> fieldsToRetrieve = new ArrayList<>();
        for (Map<String, String> field : fields) {
            if (Boolean.parseBoolean(field.get(JulieXMLConstants.RETRIEVE)))
                fieldsToRetrieve.add(field);
        }
        return fieldsToRetrieve;
    }

    /**
     * Returns the names of the columns forming the primary key in a CSV format.
     * <p>
     * This method calls {@link #getPrimaryKey()} to obtain the list of primary
     * key column names. It then builds a string consisting of these names
     * separated by commas and returns this string.
     * <p>
     * Example: If the primary key columns are "pmid" and "systemID", the string
     * returned would be "pmid,systemID".
     *
     * @return A comma separated list of the column names which form the primary
     * key in this table scheme.
     */
    public String getPrimaryKeyString() {
        return StringUtils.join(getPrimaryKey(), ",");
    }

    /**
     * Takes an array of format strings according to {@link String#format(String, Object...)} with a single %s symbol.
     * The number of format
     * string must be equal to the number of primary key elements of this table schema definition. Then, for the
     * <code>i</code>th format string, the <code>i</code>th primary key element is applied.
     *
     * @param fmtStrs The format string to fill with primary key elements, one format string per primary key element.
     * @return An array of strings corresponding to the format strings filled with the primary key elements.
     * @see {@link String#format(String, Object...)}
     */
    public String[] expandPKNames(String[] fmtStrs) {
        return JulieXMLTools.expandArrayEntries(getPrimaryKey(), fmtStrs);
    }

    /**
     * Applies each primary key element to the format string and returns the results as array. Each result element
     * corresponds to one primary key element applied to the format string.
     *
     * @param fmtStr The format string to be filled with the primary key elements.
     * @return All results corresponding to applying each primary key element once to the given format string.
     */
    public String[] expandPKNames(String fmtStr) {
        return JulieXMLTools.expandArrayEntries(getPrimaryKey(), fmtStr);
    }

    public String getTimestampFieldName() {
        return timestampFieldName;
    }

    @Override
    public String toString() {
        List<String> strList = new ArrayList<>();
        strList.add("Schema configuration for \"" + name + "\":");
        for (Map<String, String> field : fields) {
            strList.add("\n");
            strList.add("Field \"" + field.get(JulieXMLConstants.NAME) + "\":");
            for (String attr : field.keySet()) {
                strList.add(attr + "=\"" + field.get(attr) + "\"");
            }
        }
        return StringUtils.join(strList, "\n");
    }

    public String getConfigText() {
        if (configData != null)
            return new String(configData);
        return "<no XML definition available>";
    }

    public String[] getColumns() {
        if (columns == null) {
            List<String> columnNames = new ArrayList<>();
            for (Map<String, String> field : fields) {
                columnNames.add(field.get(JulieXMLConstants.NAME));
            }
            columns = new String[columnNames.size()];
            columnNames.toArray(columns);
        }
        return columns;
    }
}
