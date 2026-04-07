package com.dremio.community.excel.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;

/**
 * Generates a sample .xlsx file for testing the importer.
 * Run: java -cp dremio-excel-importer.jar com.dremio.community.excel.util.SampleDataGenerator [output.xlsx]
 */
public class SampleDataGenerator {

    public static void main(String[] args) throws IOException {
        String outputPath = args.length > 0 ? args[0] : "sample-data/sample.xlsx";
        generate(outputPath);
        System.out.println("Sample file written to: " + outputPath);
    }

    public static void generate(String outputPath) throws IOException {
        try (Workbook wb = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(outputPath)) {

            // --- Sheet 1: Sales Orders ---
            Sheet sales = wb.createSheet("Sales Orders");
            CellStyle dateStyle = wb.createCellStyle();
            CreationHelper ch = wb.getCreationHelper();
            dateStyle.setDataFormat(ch.createDataFormat().getFormat("yyyy-mm-dd"));

            // Header row
            Row header = sales.createRow(0);
            String[] headers = {"Order ID", "Customer Name", "Product", "Quantity",
                    "Unit Price", "Order Date", "Shipped", "Notes"};
            for (int i = 0; i < headers.length; i++) {
                header.createCell(i).setCellValue(headers[i]);
            }

            // Data rows
            Object[][] data = {
                    {1001L, "Acme Corp",       "Widget A",  5L,  12.99, date(2026, 1, 15),  true,  "Rush order"},
                    {1002L, "Globex Inc",      "Widget B",  12L, 8.50,  date(2026, 1, 16),  true,  null},
                    {1003L, "Initech LLC",     "Gadget X",  1L,  199.00,date(2026, 1, 18),  false, "Pending approval"},
                    {1004L, "Umbrella Corp",   "Widget A",  50L, 11.49, date(2026, 2,  1),  true,  null},
                    {1005L, "Stark Industries","Gadget Y",  3L,  349.99,date(2026, 2,  5),  true,  "Priority customer"},
                    {1006L, "Wayne Enterprises","Widget B", 25L, 8.25,  date(2026, 2, 10),  true,  null},
                    {1007L, "Hooli",           "Gadget X",  7L,  189.00,date(2026, 2, 14),  false, null},
                    {1008L, "Pied Piper",      "Widget A", 100L, 10.99, date(2026, 2, 20),  true,  "Bulk discount applied"},
                    {1009L, "Dunder Mifflin",  "Widget B",  8L,  8.50,  date(2026, 3,  1),  true,  null},
                    {1010L, "Vandelay Ind.",   "Gadget Y",  2L,  329.00,date(2026, 3,  5),  false, "Custom config"},
            };

            for (int r = 0; r < data.length; r++) {
                Row row = sales.createRow(r + 1);
                for (int c = 0; c < data[r].length; c++) {
                    Object val = data[r][c];
                    if (val == null) continue;
                    Cell cell = row.createCell(c);
                    if (val instanceof Long) {
                        cell.setCellValue((Long) val);
                    } else if (val instanceof Double) {
                        cell.setCellValue((Double) val);
                    } else if (val instanceof Boolean) {
                        cell.setCellValue((Boolean) val);
                    } else if (val instanceof Calendar) {
                        cell.setCellValue((Calendar) val);
                        cell.setCellStyle(dateStyle);
                    } else {
                        cell.setCellValue(val.toString());
                    }
                }
            }

            // Auto-size columns
            for (int i = 0; i < headers.length; i++) sales.autoSizeColumn(i);

            // --- Sheet 2: Employee Directory ---
            Sheet employees = wb.createSheet("Employees");
            Row empHeader = employees.createRow(0);
            String[] empHeaders = {"Employee ID", "First Name", "Last Name",
                    "Department", "Salary", "Start Date", "Active"};
            for (int i = 0; i < empHeaders.length; i++) {
                empHeader.createCell(i).setCellValue(empHeaders[i]);
            }

            Object[][] empData = {
                    {101L, "Alice", "Smith",   "Engineering",  95000.00, date(2020, 3, 15), true},
                    {102L, "Bob",   "Jones",   "Sales",        72000.00, date(2019, 7,  1), true},
                    {103L, "Carol", "Williams","Engineering", 105000.00, date(2021, 1, 20), true},
                    {104L, "Dave",  "Brown",   "Finance",      88000.00, date(2018, 9,  5), false},
                    {105L, "Eve",   "Davis",   "Marketing",    79000.00, date(2022, 4, 10), true},
            };

            for (int r = 0; r < empData.length; r++) {
                Row row = employees.createRow(r + 1);
                for (int c = 0; c < empData[r].length; c++) {
                    Object val = empData[r][c];
                    if (val == null) continue;
                    Cell cell = row.createCell(c);
                    if (val instanceof Long) {
                        cell.setCellValue((Long) val);
                    } else if (val instanceof Double) {
                        cell.setCellValue((Double) val);
                    } else if (val instanceof Boolean) {
                        cell.setCellValue((Boolean) val);
                    } else if (val instanceof Calendar) {
                        cell.setCellValue((Calendar) val);
                        cell.setCellStyle(dateStyle);
                    } else {
                        cell.setCellValue(val.toString());
                    }
                }
            }
            for (int i = 0; i < empHeaders.length; i++) employees.autoSizeColumn(i);

            wb.write(fos);
        }
    }

    private static Calendar date(int year, int month, int day) {
        Calendar cal = Calendar.getInstance();
        cal.set(year, month - 1, day, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }
}
