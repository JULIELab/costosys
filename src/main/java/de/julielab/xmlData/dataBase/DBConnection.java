package de.julielab.xmlData.dataBase;

import java.sql.Connection;

public class DBConnection implements AutoCloseable{
    private Connection conn;
    private DataBaseConnector dbc;

    public DBConnection(Connection conn, DataBaseConnector dbc) {
        this.conn = conn;
        this.dbc = dbc;
    }

    public Connection getConn() {
        return conn;
    }

    @Override
    public void close() throws Exception {
        dbc.releaseConnection(conn);
    }
}
