import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class Namespace {
    @Test
    public void listTableDescriptorsByNamespace() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();
        // 获取指定命名空间中的表信息
        List<TableDescriptor> google = admin.listTableDescriptorsByNamespace(Bytes.toBytes("Google"));
        for (TableDescriptor tableDescriptor : google) {

        }

        admin.close();
        conn.close();
    }
    @Test
    public void deleteNamespace() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();

        // 删除命名空间
        admin.deleteNamespace("Google");

        admin.close();
        conn.close();
    }

    @Test
    public void createNamespace() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();

        // 2. 创建命名空间
        NamespaceDescriptor nd = NamespaceDescriptor.create("Google").build();
        try {
            admin.createNamespace(nd);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已存在！");
        } catch (Exception e) {
            e.printStackTrace();
        }

        admin.close();
        conn.close();
    }

    @Test
    public void listNamespace() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }

        admin.close();
        conn.close();
    }

    @Test
    public void listNamespaceTables() throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection conn = ConnectionFactory.createConnection(cfg);

        // 1. 获取管理员对象(获取权限)
        Admin admin = conn.getAdmin();

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }

        admin.close();
        conn.close();
    }
}
