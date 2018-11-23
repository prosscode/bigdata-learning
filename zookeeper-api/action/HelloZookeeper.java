import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author pross shawn
 *
 * create time：2018年3月22日
 *
 * content：第一个zk程序
 */
public class HelloZookeeper {
	private static String connectString="hadoop02:2181,hadoop02:2181,hadoop04:2181";
	private static int sessionTimout=4000;
	private static String path; 
	
	public static void main(String[] args) throws Exception {
		//zookeeper连接
		ZooKeeper zk = new ZooKeeper(connectString,sessionTimout,null);
		
		path="/c";
//		byte[] data = zk.getData(path, null, null);
//		System.out.println(new String(data));
		
//		if(path==null){
//			System.out.println("111");
//		}else{
//			System.out.println("222");
//			byte[] data = zk.getData(path, null, null);
//			System.out.println(new String(data));
//		}
		
//		List<String> children = zk.getChildren(path, null);
//		for(String child:children){
//			System.out.println(child);
//		}
		Stat exists = zk.exists(path, false);
		System.out.println(exists);
		
		//ZK关闭
		zk.close();
	}

}
