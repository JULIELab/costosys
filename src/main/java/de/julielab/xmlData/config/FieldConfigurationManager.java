/**
 * FieldConfigurationManager.java
 *
 * Copyright (c) 2013, JULIE Lab.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Common Public License v1.0
 *
 * Author: faessler
 *
 * Current version: 1.0
 * Since version:   1.0
 *
 * Creation date: 01.02.2013
 **/

/**
 * 
 */
package de.julielab.xmlData.config;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * This class is essentially a <tt>HashMap</tt>.
 * </p>
 * <p>
 * It maps table schema names defined in the default or user provided
 * configuration to the {@link FieldConfig} objects modeling these schemas. This
 * class adds some minor validity checks to the default map methods.
 * </p>
 * 
 * @author faessler
 * 
 */
public class FieldConfigurationManager extends HashMap<String, FieldConfig> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6516109594561720970L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.HashMap#get(java.lang.Object)
	 */
	@Override
	public FieldConfig get(Object key) {
		if (null == key || StringUtils.isBlank(key.toString()))
			throw new TableSchemaDoesNotExistException(
					"No table schema name was given. A table schema must be specified in the configuration file. A predefined table schema from the default configuration or a custom definition may be used.");

		FieldConfig fieldConfig = super.get(key);
		if (null == fieldConfig) {
			throw new TableSchemaDoesNotExistException("The requested table schema definition \"" + key
					+ "\" is not defined in the default configuration or the user provided configuration.");
		}
		return fieldConfig;
	}

}
