package com.li.hbase2spark.utils;

import com.li.hbase2spark.bean.VideoPlayBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtil {

    private static Configuration conf = null;
    private static Connection connection;

    private static final String ZK = "192.168.100.2,192.168.100.3,192.168.100.4";
    private static final String CL = "2181";
    private static final String DIR = "/hbase";

    static {

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void list() throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);

        TableName[] tableNames = admin.listTableNames();

        for (int i = 0; i < tableNames.length; i++) {
            TableName tn = tableNames[i];
            System.out.println(new String(tn.getName()));
        }
        System.out.println(tableNames);
    }

    @Test
    public void testDrop() throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("test_tr_accuracy_analyze");
        admin.deleteTable("test_tr_accuracy_analyze");
        admin.close();
    }


    @Test
    public void testScan() throws Exception {
        HTablePool pool = new HTablePool(conf, 10);
        HTableInterface table = pool.getTable("test_tr_accuracy_analyze");
        Scan scan = new Scan(Bytes.toBytes("rk0001"), Bytes.toBytes("rk0002"));
        scan.addFamily(Bytes.toBytes("info"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            /**
             for(KeyValue kv : r.list()){
             String family = new String(kv.getFamily());
             System.out.println(family);
             String qualifier = new String(kv.getQualifier());
             System.out.println(qualifier);
             System.out.println(new String(kv.getValue()));
             }
             */
            byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }
        pool.close();
    }


    @Test
    public void testDel() throws Exception {
        HTable table = new HTable(conf, "test_tr_accuracy_analyze2");
        Delete del = new Delete(Bytes.toBytes("9"));
//        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.65.130");
        conf.set("hbase.zookeeper.quorum", HBaseUtil.ZK);
        conf.set("hbase.zookeeper.property.clientPort", HBaseUtil.CL);
        conf.set("hbase.rootdir", HBaseUtil.DIR);

        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor table = new HTableDescriptor(VideoPlayBean.TEST_HBASE_TABLE);

        HColumnDescriptor columnFamily = new HColumnDescriptor(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS);
        columnFamily.setMaxVersions(10);
        table.addFamily(columnFamily);

        HColumnDescriptor columnFamily2 = new HColumnDescriptor(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS2);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily2);


        admin.createTable(table);
        admin.close();

    }


    @Test
    public void testPut() throws Exception {

        HTable table = new HTable(conf, "test_tr_accuracy_analyze2");


        Put put = new Put(Bytes.toBytes("101"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("correct"), Bytes.toBytes("138"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("error"), Bytes.toBytes("113"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("sum"), Bytes.toBytes("251"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("accuracy"), Bytes.toBytes("0.55"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("submitTime"), Bytes.toBytes("2018-06-13"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("evaluationAnswerTime"), Bytes.toBytes("483"));
        put.addColumn(Bytes.toBytes("accuracy_result"), Bytes.toBytes("evaluationAnswerTime"), Bytes.toBytes("483"));

        table.put(put);
        table.close();
    }


    @Test
    public void testGet() throws Exception {
        //HTablePool pool = new HTablePool(conf, 10);
        //HTable table = (HTable) pool.getTable("user");
        HTable table = new HTable(conf, "test_tr_accuracy_analyze");
        Get get = new Get(Bytes.toBytes("26"));
        //get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.setMaxVersions(1);
        Result result = table.get(get);
        //result.getValue(family, qualifier)


        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            byte[] familyArray = c.getQualifier();
            System.out.println(new String(familyArray) + "=" + new String(c.getValue()));
        }

        table.close();
    }

    /**
     * @param clazz   转换对象class
     * @param version 查询最多多少版本
     * @param result  hbase查询结果
     * @param ismult  是否返回集合
     * @return
     */
    private void hbase2Object(Class clazz, int version, Result result, boolean ismult) {

        Field[] declaredFields = clazz.getDeclaredFields();

        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            byte[] familyArray = c.getQualifier();
            System.out.println(new String(familyArray));
        }


//        return null;
    }

    public static void put2hbase(String table, VideoPlayBean vb) {

        try {
            HTable _table = new HTable(conf, table);

            String[] columns = new String[]{
                    vb.getUsername(),
                    vb.getVideoPlayTime(),
                    vb.getVideoModulePlayTime()
            };

            Put put = new Put(Bytes.toBytes(columns[0]));

            put.addColumn(Bytes.toBytes(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS),
                    Bytes.toBytes(VideoPlayBean.HBASE_TABLE_COLUMN_VIDEOPLAYTIME),
                    Bytes.toBytes(columns[1]));

            put.addColumn(Bytes.toBytes(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS2),
                    Bytes.toBytes(VideoPlayBean.HBASE_TABLE_COLUMN_VIDEOMODULEPLAYTIME
                    ),
                    Bytes.toBytes(columns[2]));

            _table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putAll2hbase(String table, List<VideoPlayBean> playBeanList) {

        try {

            System.out.println(System.currentTimeMillis() + "----> putAll2hbase start\t" + playBeanList.size());
            HTable _table = new HTable(conf, table);

            String array[][] = new String[playBeanList.size()][VideoPlayBean.class.getDeclaredFields().length];
            int i = 0;
            for (VideoPlayBean vb : playBeanList) {

                String[] row = new String[]{
                        vb.getUsername(),
                        vb.getVideoPlayTime(),
                        vb.getVideoModulePlayTime()
                };

                array[i] = row;
                i++;
            }


            List<Put> puts = new ArrayList<>();
            List<Delete> deletes = new ArrayList<Delete>();

            for (int j = 0; j < array.length; j++) {

                String[] columns = array[j];
                Delete delete = new Delete(Bytes.toBytes(columns[0]));
                deletes.add(delete);


                Put put = new Put(Bytes.toBytes(columns[0]));

                put.addColumn(Bytes.toBytes(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(VideoPlayBean.HBASE_TABLE_COLUMN_VIDEOPLAYTIME),
                        Bytes.toBytes(columns[1]));

                put.addColumn(Bytes.toBytes(VideoPlayBean.HBASE_TABLE_FAMILY_COLUMNS),
                        Bytes.toBytes(VideoPlayBean.HBASE_TABLE_COLUMN_VIDEOMODULEPLAYTIME),
                        Bytes.toBytes(columns[2]));


                puts.add(put);
            }

//            _table.delete(deletes);

            _table.put(puts);
            System.out.println(System.currentTimeMillis() + "----> putAll2hbase finish\t" + playBeanList.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//
//        System.out.println(AccuracyEntity.class.getDeclaredFields().length);
//    }


}
