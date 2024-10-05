package org.bamboo;

import java.util.List;

/**
 * ColumnFamily represents a collection of columns in a table.
 */
public class ColumnFamily {
    private final String name;
    private final List<String> columnNames;
    private String path;

    private String format = "parquet";

    public ColumnFamily(String name, List<String> columns) {
        this(name, columns, "parquet");
    }

    public ColumnFamily(String name, List<String> columns, String format) {
        this(name, columns, format, null);
    }

    public ColumnFamily(String name, List<String> columns, String format, String path) {
        this.name = name;
        this.columnNames = columns;
        this.format = format;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getPath() {
        return path;
    }

    public String getFormat() {
        return format;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
