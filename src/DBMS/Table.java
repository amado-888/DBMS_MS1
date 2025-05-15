package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable {

    private String name;
    private String[] columnsNames;
    private int lastPageNumber;
    private int totalNumberofRecords;

    public Table(String name, String[] columnsNames) {
        this.name = name;
        this.columnsNames = columnsNames;
        this.lastPageNumber = 0;
        this.totalNumberofRecords = 0;

    }

    public String getName() {
        return name;
    }

    public String[] getColumnsNames() {
        return columnsNames;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setColumnsNames(String[] columnsNames) {
        this.columnsNames = columnsNames;
    }

    public void setLastPageNumber(int lastPageNumber) {
        this.lastPageNumber = lastPageNumber;
    }

    public int getLastPageNumber() {
        return lastPageNumber;
    }

    public int getTotalNumberofRecords() {
        return totalNumberofRecords;
    }

    public void setTotalNumberofRecords(int totalNumberofRecords) {
        this.totalNumberofRecords = totalNumberofRecords;
    }
}
