/**
 * DataBaseConnectorTest.java
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
 * Creation date: 06.04.2011
 **/

package de.julielab.costosys.configuration;

import com.ximpleware.VTDException;
import de.julielab.xml.JulieXMLConstants;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.*;
/**
 * Tests for methods and general functionality of the DataBaseConnector which
 * does not require a database.
 *
 * @author faessler
 */
public class ConfigReaderTest {

    // These tests compares a merged configuration whose correctness
    // has been manually been checked to the merged configuration
    // actually output be the algorithm.
    // This particular tests also checks whether the active PG schema is correct
    // (it uses the only user configuration defining a custom Postgres schema).
    @Test
    public void testMergeConfigDataWithAll() throws Exception {
        InputStream is;
        byte[] defaultConf;
        byte[] userConf;
        byte[] mergedConf;
        byte[] mergedConfCorrect;

        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/testDefaultConfiguration.xml");
        defaultConf = IOUtils.toByteArray(is);

        // Get user configuration which defines a schema and a DB connection.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        userConf = IOUtils.toByteArray(is);

        // Merge default and user configuration.
        Method mergeConfigData = de.julielab.costosys.configuration.ConfigReader.class.getDeclaredMethod(
                "mergeConfigData", byte[].class, byte[].class);
        mergeConfigData.setAccessible(true);
        mergedConf = (byte[]) mergeConfigData.invoke(null, defaultConf,
                userConf);

        // Check whether the result matches the correct version.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/mergedConfCorrectWithAll.xml");
        mergedConfCorrect = IOUtils.toByteArray(is);

        assertEquals(new String(mergedConfCorrect), new String(mergedConf));
    }

    @Test
    public void testMergeConfigDataWithoutSchema() throws Exception {
        // Now check with a configuration without a schema definition. --------
        InputStream is;
        byte[] defaultConf;
        byte[] userConf;
        byte[] mergedConf;
        byte[] mergedConfCorrect;

        Method mergeConfigData = de.julielab.costosys.configuration.ConfigReader.class.getDeclaredMethod(
                "mergeConfigData", byte[].class, byte[].class);
        mergeConfigData.setAccessible(true);
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/testDefaultConfiguration.xml");
        defaultConf = IOUtils.toByteArray(is);

        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithoutSchema.xml");
        userConf = IOUtils.toByteArray(is);

        // Merge default and user configuration.
        mergedConf = (byte[]) mergeConfigData.invoke(new de.julielab.costosys.configuration.ConfigReader(null),
                defaultConf, userConf);

        // Check whether the result matches the correct version.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/mergedConfCorrectWithoutSchema.xml");
        mergedConfCorrect = IOUtils.toByteArray(is);

        assertEquals(new String(mergedConfCorrect), new String(mergedConf));
    }

    @Test
    public void testMergeConfigDataWithoutDB() throws Exception {
        // Now check with a configuration without a schema definition. --------
        InputStream is;
        byte[] defaultConf;
        byte[] userConf;
        byte[] mergedConf;
        byte[] mergedConfCorrect;

        Method mergeConfigData = de.julielab.costosys.configuration.ConfigReader.class.getDeclaredMethod(
                "mergeConfigData", byte[].class, byte[].class);
        mergeConfigData.setAccessible(true);
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/testDefaultConfiguration.xml");
        defaultConf = IOUtils.toByteArray(is);

        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithoutDB.xml");
        userConf = IOUtils.toByteArray(is);

        // Merge default and user configuration.
        mergedConf = (byte[]) mergeConfigData.invoke(new de.julielab.costosys.configuration.ConfigReader(null),
                defaultConf, userConf);

        // Check whether the result matches the correct version.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/mergedConfCorrectWithoutDB.xml");
        mergedConfCorrect = IOUtils.toByteArray(is);

        assertEquals(new String(mergedConfCorrect), new String(mergedConf));
    }

    @Test
    public void dbConfigTest() throws Exception {
        InputStream is = null;
        // Just read in any configuration defining a database connection.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        de.julielab.costosys.configuration.DBConfig dbconf = new DBConfig(IOUtils.toByteArray(is));
        assertEquals("jdbc:postgresql://aserver.net/aDB", dbconf.getUrl());
        assertEquals("anotherschema", dbconf.getActiveDataPGSchema());
    }

    @Test
    public void fieldConfigTest() throws Exception {
        InputStream is = null;
        // Just read in any configuration defining a database table schema.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        byte[] config = IOUtils.toByteArray(is);
        String activeSchemaName = ConfigBase.getActiveConfig(config,
                de.julielab.costosys.configuration.ConfigReader.XPATH_ACTIVE_TABLE_SCHEMA);
        de.julielab.costosys.configuration.FieldConfig fc = new FieldConfig(config, activeSchemaName);
        List<Map<String, String>> fields = fc.getFields();

        Map<String, String> field = fields.get(0);
        String attrVal;

        attrVal = field.get(JulieXMLConstants.NAME);
        assertEquals("field1Schema1", attrVal);
        attrVal = field.get(JulieXMLConstants.TYPE);
        assertEquals("text", attrVal);
        attrVal = field.get(JulieXMLConstants.XPATH);
        assertEquals("path/to/value1", attrVal);
        attrVal = field.get(JulieXMLConstants.PRIMARY_KEY);
        assertEquals("true", attrVal);
        attrVal = field.get(JulieXMLConstants.RETRIEVE);
        assertEquals("true", attrVal);

        field = fields.get(1);
        attrVal = field.get(JulieXMLConstants.NAME);
        assertEquals("field2Schema1", attrVal);
        attrVal = field.get(JulieXMLConstants.TYPE);
        assertEquals("text", attrVal);
        attrVal = field.get(JulieXMLConstants.XPATH);
        assertEquals("path/to/value2", attrVal);
        attrVal = field.get(JulieXMLConstants.RETURN_XML_FRAGMENT);
        assertEquals("true", attrVal);
        attrVal = field.get(JulieXMLConstants.RETRIEVE);
        assertEquals("true", attrVal);
    }

    @Test
    public void configReaderTest() {
        InputStream is = null;
        @SuppressWarnings("unused")
        de.julielab.costosys.configuration.ConfigReader cr = null;
        // It is valid not to deliver a user configuration at all. The default
        // should be used. This shouldn't raise any error.
        cr = new de.julielab.costosys.configuration.ConfigReader(is);

        // Now load a quite normal schema.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        // First check whether the file exists in case someone did rename it.
        assertTrue(is != null);

        // Now check whether the merging of configurations without errors.
        cr = new de.julielab.costosys.configuration.ConfigReader(is);

        // Repeat with different kinds of configurations.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithoutSchema.xml");
        assertTrue(is != null);
        cr = new de.julielab.costosys.configuration.ConfigReader(is);
        // ----------------------------

        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithoutDB.xml");
        assertTrue(is != null);

        cr = new de.julielab.costosys.configuration.ConfigReader(is);
    }

    @Test
    public void testGetAllSchemaNames() throws Exception {
        // Just read any configuration because we need some instance of
        // ConfigReader.
        InputStream is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        de.julielab.costosys.configuration.ConfigReader cr = new ConfigReader(is);

        Method getSchemaNamesMethod = cr.getClass().getDeclaredMethod(
                "getAllSchemaNames", byte[].class);
        getSchemaNamesMethod.setAccessible(true);

        // Now read a configuration we actually want to test against. We could
        // of course get the merged configuration from the ConfigReader
        // instance, but for a test a smaller (not-merged) configuration is more
        // suitable.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithAll.xml");
        byte[] configuration = IOUtils.toByteArray(is);
        @SuppressWarnings("unchecked")
        List<String> schemaNames = (List<String>) getSchemaNamesMethod.invoke(
                cr, configuration);

        assertEquals(2, schemaNames.size());
        assertEquals("userTableSchema1", schemaNames.get(0));
        assertEquals("userTableSchema2", schemaNames.get(1));
    }

    @Test
    public void testGetTypeSystemFiles() throws Exception {
        InputStream is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithTypeSystem.xml");
        de.julielab.costosys.configuration.ConfigReader cr = new ConfigReader(is);

        List<File> typeSystemFiles = cr.getTypeSystemFiles();
        assertThat(typeSystemFiles).extracting(File::getName).containsExactly("tsone.xml", "tstwo.xml", "tsthree.xml");

        Method getTypeSystemPathsMethod = cr.getClass().getDeclaredMethod(
                "getTypeSystemPaths", byte[].class);
        getTypeSystemPathsMethod.setAccessible(true);

        // Now read a configuration we actually want to test against. We could
        // of course get the merged configuration from the ConfigReader
        // instance, but for a test a smaller (not-merged) configuration is more
        // suitable.
        is = ConfigReaderTest.class
                .getResourceAsStream("/configuration/confWithTypeSystem.xml");
        byte[] configuration = IOUtils.toByteArray(is);
        @SuppressWarnings("unchecked")
        List<String> typeSystemPaths = (List<String>) getTypeSystemPathsMethod.invoke(
                cr, configuration);

        assertEquals(3, typeSystemPaths.size());
        assertEquals("tsone.xml", typeSystemPaths.get(0));
        assertEquals("tstwo.xml", typeSystemPaths.get(1));
        assertEquals("tsthree.xml", typeSystemPaths.get(2));
    }
}
