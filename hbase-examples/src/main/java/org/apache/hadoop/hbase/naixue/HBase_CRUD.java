package org.apache.hadoop.hbase.naixue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/9/7 15:36
 * Description：
 */
public class HBase_CRUD {

    public static void main(String[] args) {
        // 请开始你的表演！
        try {
            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 获取 HBase 链接
             *   HBaseConfiguration.create(); 会去加载 hbase-default.xml 和  hbase-site.xml 配置文件
             */
            Configuration config = HBaseConfiguration.create();
            // 通过反射构造一个 ConnectionImplementation 对象
            Connection connection = ConnectionFactory.createConnection(config);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 获取管理员对象
             */
            Admin admin = connection.getAdmin();
            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 创建表
             */
            admin.createTable(null);

            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： get 一条数据
             *   HTable user 是一个 Table 对象，它的内部维护了一个 connection 链接对象
             */
            Table user = connection.getTable(TableName.valueOf("user"));
            Get get = new Get("rk01".getBytes());
            user.get(get);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释：get 一批数据
             */

            List<Get> gets = new ArrayList<>();
            gets.add(get);
            user.get(gets);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 插入一条数据
             *   Put 对象中，维持了一个 NavigableMap
             *   其中 key 是 列簇
             *        value 是 List<Cell>, Cell 对象就是一个 Key-Value
             */
            Put put = new Put(Bytes.toBytes("rk01"));
            put.addColumn("family".getBytes(), "qualifier".getBytes(), "value".getBytes());
            user.put(put);    // Put流程的入口！


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 插入一批数据
             */
            List<Put> puts = new ArrayList<Put>();
            puts.add(put);
            user.put(puts);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 删除一条数据
             */
            Delete delete = new Delete(Bytes.toBytes("rk01"));
            user.delete(delete);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 删除一批数据
             */
            List<Delete> deletes = new ArrayList<Delete>();
            deletes.add(delete);
            user.delete(deletes);


            /********
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *   注释： 范围扫描
             */
            Scan scan = new Scan().withStartRow("".getBytes()).withStopRow("".getBytes());
            ResultScanner scanner = user.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while(iterator.hasNext()){
                Result next = iterator.next();
                System.out.println(next.toString());
            }

        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
