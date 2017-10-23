import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cwj on 17-10-19.
 *
 */
public class HbaseCRUDTest {

    private Configuration configuration;
    private Connection connection;

    private String tableName;
    private String rowKey;
    private String startRowKey;
    private String stopRowKey;
    private String colFamily;
    private String col;
    private String value;

    private Logger logger = LogManager.getLogger(HbaseCRUDTest.class);

    @Before
    public void setUp() {
        configuration = HBaseConfiguration.create();
        tableName = "ccc";
        rowKey = "row1";
        startRowKey = "row1";
        stopRowKey = "row3";
        colFamily = "info";
        col = "name";
        value = "cwj";

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTableTest() throws Exception {
        logger.info("start create table...");

        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(tableName)) {
            logger.warn(tableName + " is exist!");
            System.exit(0);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
        admin.createTable(tableDescriptor);

        logger.info("create table " + tableName + " successful!");
    }

    @Test
    public void insertTest() throws Exception {
        logger.info("start insert data...");
        HTable table = new HTable(configuration, tableName);
        //一个PUT代表一行数据，再NEW一个PUT表示第二行数据，每行rowKey唯一，可以在构造方法中传入；
        Put put = new Put("row1".getBytes());
        put.add(colFamily.getBytes(), col.getBytes(), value.getBytes());
        table.put(put);
        logger.info("put " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", " + value + " successful!");
    }

    @Test
    public void appendTest() throws Exception {
        logger.info("start the append!");
        HTable table = new HTable(configuration, tableName);
        Append append = new Append(rowKey.getBytes());
        append.add(colFamily.getBytes(), col.getBytes(), value.getBytes());
        table.append(append);
        logger.info("append " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", " + value + " successful!");
    }

    @Test
    public void deleteRowTest() throws Exception {
        logger.info("start delete row...");
        HTable table = new HTable(configuration, tableName);
        List list = new ArrayList();//删除多行时可将多个delete操作放入一个list中
        Delete delete = new Delete(rowKey.getBytes());
        list.add(delete);
        table.delete(list);
        logger.info("delete row successful! ");
    }

    @Test
    public void deleteColumnTest() throws Exception {
        logger.info("start delete column...");
        HTable table = new HTable(configuration, tableName);
        Delete deleteCol = new Delete(rowKey.getBytes());
        deleteCol.deleteColumns(colFamily.getBytes(), col.getBytes());
        table.delete(deleteCol);
        logger.info("delete " + tableName + ", " + rowKey + ", " + colFamily + ":" + col + ", successful!");
    }

    @Test
    public void deleteTableTest() throws Exception {
        logger.info("start delete table...");
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            logger.warn(tableName + " is not exist!");
            System.exit(0);
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        logger.info("delete " + tableName + " successful! ");
    }

    @Test
    public void getRawTest() throws Exception {
        HTable table = new HTable(configuration, tableName);
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        Cell r = result.listCells().get(0);
        logger.info("row: " + Bytes.toString(CellUtil.cloneRow(r)));
        logger.info("colFamily: " + Bytes.toString(CellUtil.cloneFamily(r)));
        logger.info("col: " + Bytes.toString(CellUtil.cloneQualifier(r)));
        logger.info("value: " + Bytes.toString(CellUtil.cloneValue(r)));
    }

    @Test
    public void scanTest() throws Exception {
        HTable table = new HTable(configuration, tableName);
        Scan scan = new Scan(startRowKey.getBytes());
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            List<Cell> cs = r.listCells();
            for (Cell cell : cs) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String colFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                logger.info("rowKey: " + rowKey + " colFamily: " + colFamily + " col: " + col +
                        " value: " + value + " timestamp: " + timestamp);
            }
        }
    }

    @Test
    public void scanByFilterTest() throws Exception{
        HTable table = new HTable(configuration, tableName);
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("row1".getBytes()));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
           logger.info(r);
        }
    }
}