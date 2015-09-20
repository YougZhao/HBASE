package Chapter3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;

public class PutExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		System.out.println("ok1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		System.out.println("ok2");
		conf.set("hbase.zookeeper.quorum", "localhost");
		System.out.println("ok3");
		Connection connection = ConnectionFactory.createConnection(conf); 
		Admin admin = connection.getAdmin();
		System.out.println("ok4");
		//Table table = connection.getTable(TableName.valueOf("test")); 
		TableName userTable = TableName.valueOf("user");
		HTableDescriptor tableDescr = new HTableDescriptor(userTable);
		tableDescr.addFamily(new HColumnDescriptor("basic".getBytes()));
		admin.createTable(tableDescr);
		System.out.println("ok5");
		//Get g = new Get("".getBytes());
		//HTable table = new HTable(conf, "testtable");
		//Put put = new Put(Bytes.toBytes("row1"));
		//put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
		//put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"),Bytes.toBytes("val2"));
		//table.put(put);
		connection.close();
	}
}