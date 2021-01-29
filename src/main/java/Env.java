import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Env {
    public static void main(String[] args) throws IOException {
        // Java => JDBC => Mysql
        // Java => API => HBase
        // 创建基本配置对象
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        // 获取Hbase的连接对象
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 关闭连接
        conn.close();
    }
}
