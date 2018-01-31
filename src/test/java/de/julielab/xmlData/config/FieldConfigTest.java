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
package de.julielab.xmlData.config;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.ximpleware.VTDException;

import de.julielab.xml.JulieXMLConstants;

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
		FieldConfig fieldConfig = new FieldConfig(configData, "userTableSchema1");
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
		fieldConfig = new FieldConfig(configData, "userTableSchema2");
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
	
}

