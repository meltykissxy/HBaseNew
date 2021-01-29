import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class Table {
    @Test
    public void createTable() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // TODO 对命名空间进行操作
        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();

        // 2. 判断表是否存在 tableExists
        // hbase存储数据时，都是按照字节码存储的，所以需要将数据转换成对应的字节码
        // 如果表名未添加命名空间，那么会默认指向default命名空间
        //TableName tableName = TableName.valueOf("user");
        TableName tableName = TableName.valueOf("Google:user");

        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在");
        } else {
            System.out.println("表未存在");
            // 3. 创建表
            // create 'namespace:table','列族'
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            // 设定列族
            // 方式1:
            // tableDescriptorBuilder.setColumnFamily(new HColumnDescriptor("info"));
            // String => Byte[]
            // 方式2：
            // 采用hbase提供的工具类将string转换为字节码
            tableDescriptorBuilder.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build()
            );
            tableDescriptorBuilder.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("detail")).build()
            );

            TableDescriptor table = tableDescriptorBuilder.build();

            admin.createTable(table);
            System.out.println("表创建成功");
        }

        conn.close();
    }

    @Test
    public void deleteTable() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("atguigu:user");

        if ( admin.tableExists(tableName) ) {
            System.out.println("表已经存在");
            // TODO 删除表
            // 删除表之前需要将表禁用
            // TableNotDisabledException
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.close();
        conn.close();
    }
}
