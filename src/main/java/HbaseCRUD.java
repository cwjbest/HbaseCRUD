import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cwj on 17-10-19.
 *
 */
public class HbaseCRUD {
    public static Configuration configuration;
    public static Connection connection;

    static {
        configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * create table
     */
    public static void createTable(String tableName) {
        System.out.println("start create table...");
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(tableName)) {
                System.out.println(tableName + " is exist!");
                System.exit(0);
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor("info"));
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("create table " + tableName + " successful!");
    }

    /**
     * insert data
     */
    public static void insert(String tableName, String rowKey, String colFamily, String col, String value) {
        System.out.println("start insert data...");
        try {
            HTable table = new HTable(configuration, tableName);
            //一个PUT代表一行数据，再NEW一个PUT表示第二行数据，每行rowKey唯一，可以在构造方法中传入；
            Put put = new Put(rowKey.getBytes());
            put.add(colFamily.getBytes(), col.getBytes(), value.getBytes());
            table.put(put);
            System.out.println("put " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * append data
     * difference between put:
     * put is update the row,if the row is not exist,it will new a row
     * append is add the new value behind the old row,not update, just like "cwj".append("123") = "cwj123";
     */
    public static void append(String rowKey, String tableName, String colFamily, String col, String value) {
        System.out.println("start the append!");
        try {
            HTable table = new HTable(configuration, tableName);
            Append append = new Append(rowKey.getBytes());
            append.add(colFamily.getBytes(), col.getBytes(), value.getBytes());
            table.append(append);
            System.out.println("append " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", " + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * delete row by rowKey
     */
    public static void deleteRow(String tableName, String rowKey) {
        System.out.println("start delete row...");
        try {
            HTable table = new HTable(configuration, tableName);
            List list = new ArrayList();
            Delete delete = new Delete(rowKey.getBytes());
            list.add(delete);
            table.delete(list);
            System.out.println("delete row successful!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * delete column by rowKey
     */
    public static void deleteColumn(String tableName, String rowKey, String colFamily, String col) {
        System.out.println("start delete column...");
        try {
            HTable table = new HTable(configuration, tableName);
            Delete deleteCol = new Delete(rowKey.getBytes());
            deleteCol.deleteColumns(colFamily.getBytes(), col.getBytes());
            table.delete(deleteCol);
            System.out.println("delete " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", successful!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * delete table
     */
    public static void deleteTable(String tableName) {
        System.out.println("start delete table...");
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                System.out.println(tableName + " is not exist!");
                System.exit(0);
            }
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete " + tableName + "successful!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * get one row
     */
    public static void getRow(String tableName, String rowKey) {
        try {
            HTable table = new HTable(configuration, tableName);
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            Cell r = result.listCells().get(0);
            System.out.print(("rowKey: " + Bytes.toString(CellUtil.cloneRow(r))));
            System.out.print(("colFamily: " + Bytes.toString(CellUtil.cloneFamily(r))));
            System.out.print(("col: " + Bytes.toString(CellUtil.cloneQualifier(r))));
            System.out.println(("value: " + Bytes.toString(CellUtil.cloneValue(r))));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * scan from startRow to stopRow
     * ex: 1~3, print 1,2
     * if only startRow, startRow~end
     */
    public static void scan(String tableName, String startRow, String stopRow) {
        try {
            HTable table = new HTable(configuration, tableName);
            Scan scan = new Scan(startRow.getBytes(), stopRow.getBytes());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result r : resultScanner) {
                List<Cell> cs = r.listCells();
                for (Cell cell : cs) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    long timestamp = cell.getTimestamp();
                    String colFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("rowKey: " + rowKey + " colFamily: " + colFamily + " col: " + col +
                            " value: " + value + " timestamp: " + timestamp);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
