import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author pross shawn
 *
 * create time：2018年3月22日
 *
 * content：监听测试
 */
public class ListeningProcessClient {
	private static String connectString="hadoop02:2181,hadoop03:2181,hadoop04:2181";
	private static int sessionTimeout=4000;
	
	public static void main(String[] args) throws Exception {
		// 连接zk
		ZooKeeper zk=new ZooKeeper(connectString, sessionTimeout,null);
		
		// 客户端创建了一个子节点，会触发NodeChildrenChanged事件
        String path = zk.create("/xyz","xyz".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
        zk.close();
		
	}

}
