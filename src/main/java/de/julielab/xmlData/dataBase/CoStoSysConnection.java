package de.julielab.xmlData.dataBase;

import java.sql.Connection;

public class CoStoSysConnection implements AutoCloseable {
    private DataBaseConnector dbc;
    private Connection connection;
    private boolean newlyReserved;

    public CoStoSysConnection(DataBaseConnector dbc, Connection connection, boolean newlyReserved) {
        this.dbc = dbc;

        this.connection = connection;
        this.newlyReserved = newlyReserved;
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean isNewlyReserved() {
        return newlyReserved;
    }

    public void release() {
        dbc.releaseConnection(this);
    }

    @Override
    public void close() {
        release();
    }
}
