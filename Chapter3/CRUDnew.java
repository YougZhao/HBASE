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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CRUDnew {
	public static void main(String args[]) throws IOException {
		//connection to hbase
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "localhost");
		Connection connection = ConnectionFactory.createConnection(conf);
		System.out.println("connection established");
		
		//create a new table
		Admin admin = connection.getAdmin();
		TableName userTable = TableName.valueOf("testtable_youg");
		HTableDescriptor tableDescr = new HTableDescriptor(userTable);
		tableDescr.addFamily(new HColumnDescriptor("colfam1".getBytes()));
		admin.createTable(tableDescr);
		System.out.println("table \"testtable\" created");
		
		//connection to a table
		Table table = connection.getTable(TableName.valueOf("testtable_youg"));
		
		//put or update data to the table
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
		table.put(put);
		System.out.println("data puted into the table");
		//put a list of data to the table
		
		//read data from the table
		Get get = new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));
		get.addColumn(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"));
		Result result = table.get(get);
		System.out.println("Get result: "+result);
		byte[] val1 = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		byte[] val2 = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
		System.out.println("Value only: "+Bytes.toString(val1));
		System.out.println("Value only: "+Bytes.toString(val2));
		System.out.println("data geted from the table");
		
		//delete data from the table
		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
		table.delete(delete);
		System.out.println("data deleted from the table");
		
		//scan the table
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result2 : scanner){
			System.out.println("Scan: "+ result2);
		}
		System.out.println("complete scan the table");
		
		//close the connection
		connection.close();
	}
}
