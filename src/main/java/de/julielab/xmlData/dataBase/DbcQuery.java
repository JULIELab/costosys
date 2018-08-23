package de.julielab.xmlData.dataBase;

public interface DbcQuery<T> {
    T query(DataBaseConnector dbc) throws Throwable;
}
