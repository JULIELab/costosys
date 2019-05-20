/**
 * DBCIterator.java
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
 * Creation date: 13.01.2012
 **/

/**
 * 
 */
package de.julielab.costosys.dbconnection;

import java.util.Iterator;

/**
 * <p>
 * Abstract class for iterators returned by the <code>DataBaseConnector</code>
 * which hold JDBC Connection objects. This class defines a method
 * {@link #close()} which should be implemented to free employed resources
 * such as database connections, threads etc.
 * </p>
 * 
 * @author faessler
 * 
 */
public abstract class DBCIterator<E> implements Iterator<E> {
	/**
	 * Frees resources occupied by this iterator (e.g. database connections).
	 */
	public abstract void close();
}
