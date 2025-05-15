package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable {
    private String tableName;
    private int size;
    private ArrayList<String[]> records;

    public Page(String tableName, int size) {
        this.tableName = tableName;
        this.size = size;
        this.records = new ArrayList<String[]>();
    }

    public boolean isFull() {
        return records.size() >= size;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ArrayList<String[]> getRecords() {
        return records;
    }

    public void setRecords(ArrayList<String[]> records) {
        this.records = records;
    }
}
