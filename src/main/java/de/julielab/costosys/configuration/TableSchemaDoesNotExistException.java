/**
 * TableSchemaDoesNotExistException.java
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
package de.julielab.costosys.configuration;

/**
 * @author faessler
 *
 */
public class TableSchemaDoesNotExistException extends IllegalArgumentException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8098259277945200129L;

	TableSchemaDoesNotExistException(String message) {
		super(message);
	}
	
}

