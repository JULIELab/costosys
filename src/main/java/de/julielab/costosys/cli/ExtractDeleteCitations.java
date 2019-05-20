/**
 * ExtractDeleteCitations.java
 *
 * Copyright (c) 2010, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: chew
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 14.12.2010
 **/

package de.julielab.costosys.cli;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import de.julielab.costosys.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.xml.JulieXMLConstants;
import de.julielab.xml.JulieXMLTools;

/**
 * Extracts PMIDs of deleted Medline documents from Medline Update XML batches.
 * Currently the path to the XML files is hard coded, this should be made more
 * flexible.
 * 
 * @author faessler
 */
public class ExtractDeleteCitations {

	private static final Logger LOG = LoggerFactory
			.getLogger(ExtractDeleteCitations.class);

	public static void main(String[] args) {
		extractDeletedPMIDs();
	}
	
	private static void extractDeletedPMIDs() {
		LOG.info("Starting extraction...");
		File baseDir = new File("/data/data_corpora/medline/updates");
		if (!baseDir.isDirectory()) {
			LOG.error(String.format(
					"Path %s does not point to a directory.",
					baseDir.getAbsolutePath()));
			System.exit(1);
		}
		String[] fileNames = baseDir.list(new FilenameFilter() {
			public boolean accept(File arg0, String arg1) {
				return arg1.endsWith(".gz");
			}
		});

		String forEachXpath = "/MedlineCitationSet/DeleteCitation/PMID";
		List<Map<String, String>> fields = new ArrayList<Map<String, String>>();
		Map<String, String> field = new HashMap<String, String>();
		field.put(JulieXMLConstants.NAME, Constants.PMID_FIELD_NAME);
		field.put(JulieXMLConstants.XPATH,
				"/MedlineCitationSet/DeleteCitation/PMID");
		fields.add(field);

		int bufferSize = 1000;
		for (String fileName : fileNames) {
			Iterator<Map<String, Object>> it = JulieXMLTools.constructRowIterator(
					baseDir.getAbsolutePath() + "/" + fileName, bufferSize,
					forEachXpath, fields, false);
			
			while (it.hasNext()) {
				Map<String, Object> row = it.next();
				String pmid = (String) row.get(Constants.PMID_FIELD_NAME);
				System.out.println(pmid);
			}
		}
	}
}
