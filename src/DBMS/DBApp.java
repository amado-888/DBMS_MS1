package DBMS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;

public class DBApp {

	static int dataPageSize = 2;

	public static void createTable(String tableName, String[] columnsNames) {

		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
		createTraceHelper(tableName, columnsNames);

	}

	public static void insert(String tableName, String[] record) {
		long startTime;
		long endTime;
		long executionTime;
		// int currentPage = 0;

		startTime = System.currentTimeMillis();
		Table table = FileManager.loadTable(tableName);
		int num = table.getTotalNumberofRecords();
		int lastPage = table.getLastPageNumber();
		int currLastPage = lastPage - 1;
		if (currLastPage != -1) {
			Page page = FileManager.loadTablePage(tableName, currLastPage);
			if (!page.isFull()) {
				page.getRecords().add(record);
				FileManager.storeTablePage(tableName, currLastPage, page);
				num++;
				table.setTotalNumberofRecords(num);
				FileManager.storeTable(tableName, table);
				endTime = System.currentTimeMillis();
				executionTime = endTime - startTime;
				insertTraceHelper(tableName, record, currLastPage, executionTime);
				return;
			}
		}
		// while (currentPage < lastPage) {
		// Page page = FileManager.loadTablePage(tableName, currentPage);
		// if (!page.isFull()) {
		// page.getRecords().add(record);
		// FileManager.storeTablePage(tableName, currentPage, page);
		// num++;
		// table.setTotalNumberofRecords(num);
		// FileManager.storeTable(tableName, table);
		// endTime = System.currentTimeMillis();
		// executionTime = endTime - startTime;
		// insertTraceHelper(tableName, record, currentPage, executionTime);
		// return;
		// }
		// currentPage++;
		// }
		Page newPage = new Page(tableName, dataPageSize);
		newPage.getRecords().add(record);
		FileManager.storeTablePage(tableName, lastPage, newPage);
		endTime = System.currentTimeMillis();
		executionTime = endTime - startTime;
		insertTraceHelper(tableName, record, lastPage, executionTime);
		lastPage++;
		table.setLastPageNumber(lastPage);
		num++;
		table.setTotalNumberofRecords(num);
		FileManager.storeTable(tableName, table);

	}

	public static ArrayList<String[]> select(String tableName) {
		long startTime;
		long endTime;
		long executionTime;
		startTime = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		int maxpage = t.getLastPageNumber();
		ArrayList<String[]> result = new ArrayList<String[]>();
		ArrayList<ArrayList<String[]>> result2 = new ArrayList<>();
		for (int i = 0; i < maxpage; i++) {
			Page page = FileManager.loadTablePage(tableName, i);
			result2.add(page.getRecords());
			// int lenPerPage = page.getRecords().size();
			// for (int j = 0; j < lenPerPage; j++) {
			// result.add(page.getRecords().get(j));
			// }
		}
		for (ArrayList<String[]> innerList : result2) {
			result.addAll(innerList);
		}
		endTime = System.currentTimeMillis();
		executionTime = endTime - startTime;
		SelectAllPagesHelper(tableName, maxpage, result.size(), executionTime);
		return result;
	}

	public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
		long startTime;
		long endTime;
		long executionTime;
		ArrayList<String[]> result = new ArrayList<String[]>();
		startTime = System.currentTimeMillis();
		Page page = FileManager.loadTablePage(tableName, pageNumber);
		result.add(page.getRecords().get(recordNumber));
		endTime = System.currentTimeMillis();
		executionTime = endTime - startTime;
		SelectPointerPageHelper(tableName, pageNumber, recordNumber, result.size(), executionTime);
		return result;
	}

	public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
		long startTime = System.currentTimeMillis();
		ArrayList<int[]> RecordsPerPage = new ArrayList<>();
		ArrayList<String[]> result = new ArrayList<>();

		Table t = FileManager.loadTable(tableName);
		int maxpage = t.getLastPageNumber();
		String[] allColumns = t.getColumnsNames();

		Map<String, Integer> columnIndexMap = new HashMap<>();
		for (int i = 0; i < allColumns.length; i++) {
			columnIndexMap.put(allColumns[i], i);
		}

		int[] colIndices = new int[cols.length];
		for (int i = 0; i < cols.length; i++) {
			colIndices[i] = columnIndexMap.get(cols[i]);
		}

		for (int pageNum = 0; pageNum < maxpage; pageNum++) {
			Page page = FileManager.loadTablePage(tableName, pageNum);
			int matchesInPage = 0;

			for (String[] currentRow : page.getRecords()) {
				boolean allMatch = true;
				for (int k = 0; k < cols.length; k++) {
					if (!vals[k].equals(currentRow[colIndices[k]])) {
						allMatch = false;
						break;
					}
				}
				if (allMatch) {
					result.add(currentRow);
					matchesInPage++;
				}
			}

			if (matchesInPage > 0) {
				RecordsPerPage.add(new int[] { pageNum, matchesInPage });
			}
		}

		long executionTime = System.currentTimeMillis() - startTime;
		int[][] RecordsPerPageArr = RecordsPerPage.toArray(new int[0][]);
		SelectConditionHelper(tableName, cols, vals, result.size(), RecordsPerPageArr, executionTime);
		return result;
	}

	public static String getFullTrace(String tableName) {
		StringBuilder content = new StringBuilder();
		Table t = FileManager.loadTable(tableName);
		int pages = t.getLastPageNumber();
		int records = t.getTotalNumberofRecords();
		try {
			String filename = tableName + "_trace" + ".txt";
			File myObj = new File(filename);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				content.append(myReader.nextLine()).append("\n");
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		content.append("Pages Count: ").append(pages)
				.append(", Records Count: ").append(records);

		return content.toString();
	}

	public static void createTraceHelper(String tableName, String[] columnsNames) {
		try {

			String filename = tableName + "_trace" + ".txt";
			File x = new File(filename);
			if (x.exists()) {
				x.delete();
			}
			x.createNewFile();

			FileWriter myWriter = new FileWriter(filename, true);
			myWriter.write(
					"Table created name:" + tableName + ", columnNames:" + Arrays.toString(columnsNames) + "\n");
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static void insertTraceHelper(String tableName, String[] record, int page, long executionTime) {
		try {
			String filename = tableName + "_trace" + ".txt";
			FileWriter myWriter = new FileWriter(filename, true);
			myWriter.write(
					"Inserted:" + Arrays.toString(record) + ", at page number:" + page + ", execution time (mil):"
							+ executionTime + "\n");
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

	public static void SelectAllPagesHelper(String tableName, int pages, int records, long executionTime) {
		try {
			String filename = tableName + "_trace" + ".txt";
			FileWriter myWriter = new FileWriter(filename, true);
			myWriter.write(
					"Select all pages:" + pages + ", records:" + records + ", execution time (mil):"
							+ executionTime + "\n");
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static void SelectPointerPageHelper(String tableName, int page, int recordNumber, int totalrecords,
			long executionTime) {
		try {
			String filename = tableName + "_trace" + ".txt";
			FileWriter myWriter = new FileWriter(filename, true);
			myWriter.write(
					"Select pointer page:" + page + ", record:" + recordNumber + ", Total output count:" + totalrecords
							+ ", execution time (mil):"
							+ executionTime + "\n");
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static void SelectConditionHelper(String tableName, String[] cols, String[] vals, int records,
			int[][] RecordsPerPage,
			long executionTime) {
		try {
			String filename = tableName + "_trace" + ".txt";
			FileWriter myWriter = new FileWriter(filename, true);
			myWriter.write(
					"Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) + ", Records per page:"
							+ Arrays.deepToString(RecordsPerPage) + ", records:" + records
							+ ", execution time (mil):"
							+ executionTime + "\n");
			myWriter.close();

		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	public static String getLastTrace(String tableName) {
		StringBuilder content = new StringBuilder();
		ArrayList<String> temp = new ArrayList<String>();
		Table t = FileManager.loadTable(tableName);
		int pages = t.getLastPageNumber();
		int records = t.getTotalNumberofRecords();
		try {
			String filename = tableName + "_trace" + ".txt";
			File myObj = new File(filename);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				temp.add(new String(myReader.nextLine()));
			}
			String[] arr = temp.toArray(new String[0]);
			content.append(arr[arr.length - 1]).append("\n");
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		content.append("Pages Count: ").append(pages)
				.append(", Records Count: ").append(records);

		return content.toString();
	}

	public static void main(String[] args) throws IOException {
		// String trace = DBApp.getFullTrace("rc5");
		// String[] lines = trace.split("\n");
		// System.out.println("Line count: " + lines.length);
		// System.out.println("Last line: " + lines[lines.length - 1]);
		FileManager.reset();
	}

}
