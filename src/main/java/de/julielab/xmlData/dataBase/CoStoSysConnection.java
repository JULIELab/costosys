package de.julielab.xmlData.dataBase;

import de.julielab.xmlData.dataBase.util.CoStoSysSQLRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CoStoSysConnection implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(CoStoSysConnection.class);
    private DataBaseConnector dbc;
    private Connection connection;
    private AtomicInteger numUsing;

    public CoStoSysConnection(DataBaseConnector dbc, Connection connection, boolean newlyReserved) {
        this.dbc = dbc;

        this.connection = connection;
        numUsing = new AtomicInteger(1);
        log.trace("Initial usage: Connection {} is now used {} times by thread {}", connection, numUsing.get(), Thread.currentThread().getName());
    }

    public void incrementUsageNumber() {
        numUsing.incrementAndGet();
        if (log.isTraceEnabled())
            log.trace("Increased usage by thread {}: Connection {} is now used {} times", Thread.currentThread().getName(), connection, numUsing.get());
    }

    public synchronized void release() throws SQLException {
        final int num = numUsing.decrementAndGet();
        if (log.isTraceEnabled())
            log.trace("Decreased usage by thread {}: Connection {} is now used {} times", Thread.currentThread().getName(), connection, numUsing.get());
        if (num == 0) {
            log.trace("Connection {} is not used any more and is released", connection);
            dbc.releaseConnection(this);
        }
    }


    public Connection getConnection() {
        return connection;
    }


    @Override
    public void close() {
        try {
            release();
        } catch (SQLException e) {
            throw new CoStoSysSQLRuntimeException(e);
        }
    }

    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    public void setAutoCommit(boolean b) throws SQLException {
        connection.setAutoCommit(b);
    }
}
