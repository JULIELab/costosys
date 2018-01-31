/**
 * BatchInserter.java
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
 * Creation date: 11.12.2012
 **/

/**
 * 
 */
package de.julielab.xmlData.dataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

/**
 * A helper class to automate the common batch-insert process as far as
 * possible.
 * 
 * @author faessler
 * 
 */
public class BatchInserter {

	private final int batchSize;

	private final PreparedStatement ps;
	private final Class<?>[] classes;
	private int counter;
	private boolean autoCommit;
	private boolean finished;

	private final int[] targetSqlTypes;

	/**
	 * <p>
	 * Creates an instance of <code>BatchInserter</code>. The connection which
	 * was used to create <code>ps</code> is set to
	 * <code>autoCommit=false</code>. This should not be changed while you want
	 * to do batch inserts. Calling <code>commitTail</code> resets the
	 * <code>autoCommit</code> value.
	 * </p>
	 * 
	 * @param ps
	 *            the <tt>PreparedStatement</tt> formulating the insertion
	 * @param classes
	 *            an array of classes reflecting the arguments which will be
	 *            passed to <tt>ps</tt>; is used for consistency checks
	 * @throws SQLException
	 */
	public BatchInserter(PreparedStatement ps, Class<?>[] classes,
			int[] targetSqlTypes) throws SQLException {
		this.ps = ps;
		this.classes = classes;
		this.targetSqlTypes = targetSqlTypes;
		this.counter = 0;
		this.batchSize = 1000;
		autoCommit = ps.getConnection().getAutoCommit();
		finished = false;
		ps.getConnection().setAutoCommit(false);
	}

	/**
	 * <p>
	 * Adds the given data to the batch.
	 * <p>
	 * <p>
	 * The given objects must correspond to the <tt>classes</tt> array given in
	 * the constructor in length and class types. An
	 * <tt>IllegalArgumentException</tt> will be risen otherwise.
	 * </p>
	 * 
	 * @param obs
	 * @throws SQLException
	 */
	public void addData(Object... obs) throws SQLException {
		int i = 0;
		try {
			checkFinished();
			if (obs.length != classes.length)
				throw new IllegalStateException(classes.length
						+ " objects expected for insertion into database, "
						+ obs.length + " were passed.");

			for (i = 0; i < obs.length; i++) {
				Object o = obs[i];
				
				if (null != o && !o.getClass().equals(classes[i]))
					throw new IllegalArgumentException(
							"Position "
									+ i
									+ " of the PreparedStatement expects an instance of class '"
									+ classes[i].getCanonicalName()
									+ "', however an instance of class '"
									+ o.getClass().getCanonicalName()
									+ "' has been passed.");
				if (null != targetSqlTypes)
					ps.setObject(i + 1, o, targetSqlTypes[i]);
				else
					ps.setObject(i + 1, o);
			}
			ps.addBatch();
			counter++;
			if (counter == batchSize) {
				commit();
				counter = 0;
			}
		} catch (PSQLException e) {
			throw new PSQLException(e.getMessage() + " (Parameter " + (i + 1)
					+ " (index 1-basiert))", new PSQLState(e.getSQLState()), e);
		}
	}

	/**
	 * <p>
	 * Commits to the database what has been added to the batch but not yet been
	 * committed.
	 * </p>
	 * <p>
	 * Sets the <code>autoCommit</code> value of the connection of the passed
	 * <code>PreparedStatement</code> back to what it was when creating the
	 * <code>BatchInserter</code>. Thus, the <code>BatchInserter</code> should
	 * no longer be used.
	 * </p>
	 * 
	 * @throws SQLException
	 */
	public void commitTail() throws SQLException {
		checkFinished();
		commit();
		ps.getConnection().setAutoCommit(autoCommit);
		finished = true;
	}

	/**
	 * @throws SQLException
	 * 
	 */
	private void commit() throws SQLException {
		checkFinished();
		Connection connection = ps.getConnection();
		ps.executeBatch();
		connection.commit();
	}

	private void checkFinished() {
		if (finished)
			throw new IllegalStateException(
					"This BatchInserter has finished its work and cannot be used any more.");
	}
}
