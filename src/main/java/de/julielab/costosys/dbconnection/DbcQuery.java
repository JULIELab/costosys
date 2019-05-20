package de.julielab.costosys.dbconnection;

public interface DbcQuery<T> {
    T query(DataBaseConnector dbc) throws Throwable;
}
