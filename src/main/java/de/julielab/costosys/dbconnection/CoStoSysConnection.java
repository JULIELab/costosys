package de.julielab.costosys.dbconnection;

import de.julielab.costosys.dbconnection.util.CoStoSysSQLRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CoStoSysConnection implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(CoStoSysConnection.class);
    private DataBaseConnector dbc;
    private Connection connection;
    private boolean shared;
    private AtomicInteger numUsing;
    private boolean isClosed;

    public CoStoSysConnection(DataBaseConnector dbc, Connection connection, boolean shared) {
        this.dbc = dbc;

        this.connection = connection;
        this.shared = shared;
        this.numUsing = new AtomicInteger(1);
        this.isClosed = false;
        log.trace("Initial usage: Connection {} is now used {} times by thread {}", connection, numUsing.get(), Thread.currentThread().getName());
    }

    /**
     * <p>Indicates if this connection can be shared between different code and threads.</p>
     *
     * @return True if this connection can be freely shared.
     */
    public boolean isShared() {
        return shared;
    }

    public int getUsageNumber() {
        return numUsing.get();
    }

    public boolean incrementUsageNumber() {
        int currentUsage = numUsing.incrementAndGet();
        return true;
    }

    private void decreaseUsageCounter() {
        numUsing.decrementAndGet();
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
        decreaseUsageCounter();
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
