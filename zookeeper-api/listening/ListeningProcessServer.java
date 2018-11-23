import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author pross shawn
 *
 * create time：2018年3月22日
 *
 * content：监听测试
 */
public class ListeningProcessServer {
	private static String connectString="hadoop02:2181,hadoop03:2181,hadoop04:2181";
	private static int sessionTimeout=4000;
	private static String path="/";
	static ZooKeeper zk = null;
	
	public static void main(String[] args) throws Exception {
		//监听连接
			String string = "Asd";
			zk =new ZooKeeper(connectString, sessionTimeout, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				String path = event.getPath();
				KeeperState state = event.getState();
				EventType type = event.getType();
				System.out.println(path+","+state+","+type);
				
				try {
					zk.getChildren(path, true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		//添加监听
		zk.getChildren(path, true);
		
		//等待时间
		Thread.sleep(Integer.MAX_VALUE);
//		zk.close();
		
	}

}
