package de.julielab.costosys.dbconnection;

import de.julielab.costosys.dbconnection.util.CoStoSysSQLRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class CoStoSysConnection implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(CoStoSysConnection.class);
    private DataBaseConnector dbc;
    private Connection connection;
    private boolean shared;
    private AtomicInteger numUsing;
    private boolean isClosed;

    /**
     * <p>Indicates if this connection can be shared between different code and threads.</p>
     * @return True if this connection can be freely shared.
     */
    public boolean isShared() {
        return shared;
    }

    public CoStoSysConnection(DataBaseConnector dbc, Connection connection, boolean shared) {
        this.dbc = dbc;

        this.connection = connection;
        this.shared = shared;
        this.numUsing = new AtomicInteger(1);
        this.isClosed = false;
        log.trace("Initial usage: Connection {} is now used {} times by thread {}", connection, numUsing.get(), Thread.currentThread().getName());
    }

    public int getUsageNumber() {
        return numUsing.get();
    }

    public synchronized boolean incrementUsageNumber() {
        int currentUsage = numUsing.incrementAndGet();
        // If the usage after the increment is 1, it was 0 before.
        // Thus, we have a concurrency issue where the counter had
        // already hit 0 and the internal connection may already
        // have been released via decreaseUsageCounter() below.
        // Thus, this connection cannot be used anymore.
        // Until I know that this actually an issue, don't handle it but just throw an exception to let us know.
        if (currentUsage == 1)
            throw new IllegalStateException("Connection usage counter increased but it had already been fallen to zero before.");
        if (log.isTraceEnabled())
            log.trace("Increased usage by thread {}: Connection {} is now used {} times", Thread.currentThread().getName(), connection, numUsing.get());
        return true;
    }

    private synchronized void decreaseUsageCounter() throws SQLException {
        final int num = numUsing.decrementAndGet();
        if (log.isTraceEnabled())
            log.trace("Decreased usage by thread {}: Connection {} with internal connection {} is now used {} times", Thread.currentThread().getName(), this, connection, numUsing.get());
        if (num == 0) {
            log.trace("Connection {} with internal connection {} is not used any more and is released", this, connection);
//            dbc.releaseConnection(this);
            log.trace("Connection {} with internal connection {} was successfully released.", this, connection);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    private String getCaller() {
        StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        String walk = walker.walk(frames -> frames
                .map(f -> f.getClassName() + "#" + f.getMethodName()).limit(3)
                .collect(Collectors.joining(", ")));
        return walk;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        try {
            decreaseUsageCounter();
        } catch (SQLException e) {
            throw new CoStoSysSQLRuntimeException(e);
        } finally {
            if (numUsing.get() <= 0) {
                try {
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    throw new CoStoSysSQLRuntimeException(e);
                }
                this.isClosed = true;
            }
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
