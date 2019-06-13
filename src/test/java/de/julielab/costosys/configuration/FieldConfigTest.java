/**
 * FieldConfigTest.java
 *
 * Copyright (c) 2012, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 07.01.2012
 **/

/**
 * 
 */
package de.julielab.costosys.configuration;

import com.ximpleware.VTDException;
import de.julielab.xml.JulieXMLConstants;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.julielab.costosys.configuration.FieldConfig.createField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Test whether the XML table scheme definition is correctly parsed and represented.
 * 
 * @author faessler
 *
 */
public class FieldConfigTest {

	@Test
	public void testBuildFields() throws SecurityException, NoSuchMethodException, FileNotFoundException, IOException, VTDException {
		byte[] configData = IOUtils.toByteArray(new FileInputStream("src/test/resources/configuration/confWithAll.xml"));
		// Test schema without explicit field closing tags.
		de.julielab.costosys.configuration.FieldConfig fieldConfig = new de.julielab.costosys.configuration.FieldConfig(configData, "userTableSchema1");
		List<Map<String,String>> fields = fieldConfig.getFields();
		Map<String, String> field1 = fields.get(0);
		Map<String, String> field2 = fields.get(1);

		assertEquals("/MedlineCitationSet/MedlineCitation", fieldConfig.getForEachXPath());
		assertEquals(2, fields.size());
		
		assertEquals("field1Schema1", field1.get(JulieXMLConstants.NAME));
		assertEquals("text", field1.get(JulieXMLConstants.TYPE));
		assertEquals("path/to/value1", field1.get(JulieXMLConstants.XPATH));
		assertEquals("true", field1.get(JulieXMLConstants.PRIMARY_KEY));
		assertEquals("true", field1.get(JulieXMLConstants.RETRIEVE));
		
		assertEquals("field2Schema1", field2.get(JulieXMLConstants.NAME));
		assertEquals("text", field2.get(JulieXMLConstants.TYPE));
		assertEquals("path/to/value2", field2.get(JulieXMLConstants.XPATH));
		assertEquals("true", field2.get(JulieXMLConstants.RETURN_XML_FRAGMENT));
		assertEquals("true", field2.get(JulieXMLConstants.RETRIEVE));
		
		// Test schema with explicit field closing tags.
		fieldConfig = new de.julielab.costosys.configuration.FieldConfig(configData, "userTableSchema2");
		fields = fieldConfig.getFields();
		field1 = fields.get(0);
		field2 = fields.get(1);

		assertEquals("/muh/maeh", fieldConfig.getForEachXPath());
		assertEquals(2, fields.size());
		
		assertEquals("field1Schema2", field1.get(JulieXMLConstants.NAME));
		assertEquals("text", field1.get(JulieXMLConstants.TYPE));
		assertEquals("PMID", field1.get(JulieXMLConstants.XPATH));
		assertEquals("true", field1.get(JulieXMLConstants.PRIMARY_KEY));
		assertEquals("true", field1.get(JulieXMLConstants.RETRIEVE));
		
		assertEquals("field2Schema2", field2.get(JulieXMLConstants.NAME));
		assertEquals("text", field2.get(JulieXMLConstants.TYPE));
		assertEquals(".", field2.get(JulieXMLConstants.XPATH));
		assertEquals("true", field2.get(JulieXMLConstants.RETURN_XML_FRAGMENT));
		assertEquals("true", field2.get(JulieXMLConstants.RETRIEVE));
		assertEquals("true", field2.get(JulieXMLConstants.PRIMARY_KEY));
	}

	@Test
	public void testIncompleteProgramaticallyDefinedFieldConfig() {
	    // test that incomplete field definitions are caught
		List<Map<String, String>> fields = new ArrayList<>();
		Map<String, String> field = new HashMap<>();
		field.put(JulieXMLConstants.NAME, "field1");
		fields.add(field);
		// The type property is missing
        assertThatThrownBy(() -> new de.julielab.costosys.configuration.FieldConfig(fields, "", "testschema")).hasMessageContaining("required \"" + JulieXMLConstants.TYPE + "\" property");
        field.remove(JulieXMLConstants.NAME);
        field.put(JulieXMLConstants.TYPE, "type1");
        // Now the name property is missing
        assertThatThrownBy(() -> new de.julielab.costosys.configuration.FieldConfig(fields, "", "testschema")).hasMessageContaining("required \"" + JulieXMLConstants.NAME + "\" property");
	}

	@Test
    public void testProgrammaticallyDefinedFieldConfig() {
        // test that incomplete field definitions are caught
        List<Map<String, String>> fields = new ArrayList<>();
        fields.add(createField(JulieXMLConstants.NAME, "field1",JulieXMLConstants.TYPE, "type1", JulieXMLConstants.PRIMARY_KEY, "true"));
        fields.add(createField(JulieXMLConstants.NAME, "field2", JulieXMLConstants.TYPE, "type2", JulieXMLConstants.PRIMARY_KEY, "true"));
        fields.add(createField(JulieXMLConstants.NAME, "field3",JulieXMLConstants.TYPE, "type3", JulieXMLConstants.RETRIEVE, "true"));
        de.julielab.costosys.configuration.FieldConfig config = new FieldConfig(fields, "foreach", "testschema");
        assertThat(config.getPrimaryKeyString()).isEqualToIgnoringWhitespace("field1,field2");
        assertThat(config.getColumnsToRetrieve()).isEqualTo(new String[]{"field3"});
        assertThat(config.getForEachXPath()).isEqualTo("foreach");
    }
}

