import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 编程思维训练
 * 1、级联查看某节点下所有节点及节点值 
 * 2、删除一个节点，不管有有没有任何子节点 
 * 3、级联创建任意节点 
 * 4、清空子节点
 */
public class ZKHomeWork {
	private static String connectString = "hadoop02:2181,hadoop02:2181,hadoop04:2181";
	private static int sessionTimout = 4000;
	private static String path;

	public static void main(String[] args) throws Exception {
		// zookeeper连接
		ZooKeeper zk = new ZooKeeper(connectString, sessionTimout, null);

		/*
		 * 级联查看某节点下所有节点及节点值
		 */
		// path="/c";
		// Map<String, String> map = ZKHomeWork.getChildNodeAndValue(path, zk);
		// for(Map.Entry<String, String> entry:map.entrySet()){
		// System.out.println(entry.getKey()+":"+entry.getValue());
		// }
		// ZKHomeWork.getChildNodeAndValue(path, zk);

		/*
		 * 删除一个节点，不管有有没有任何子节点
		 */
		 path="/c";
		 boolean rmr = ZKHomeWork.rmr(path, zk);
		 if(rmr){
		 System.out.println("删除成功！");
		 }else{
		 System.out.println("删除失败！");
		 }

		/*
		 * 级联创建任意节点
		 */
//		boolean createZNode = ZKHomeWork.createZNode("/spark", "spark", zk);
//		if (createZNode) {
//			System.out.println("联级创建成功");
//		} else {
//			System.out.println("联级创建失败");
//		}

		/*
		 * 清空子节点
		 */
		// path="/c";
		// boolean clearChildNode = ZKHomeWork.clearChildNode(path, zk);
		// if(clearChildNode){
		// System.out.println("子节点清空成功！");
		// }else{
		// System.out.println("子节点清空失败！");
		// }

		// ZK关闭
		zk.close();

	}

	/**
	 * 级联查看某节点下所有节点及节点值
	 * 
	 * @throws Exception
	 * @throws KeeperException
	 */
	public static Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk)
			throws KeeperException, Exception {
		HashMap<String, String> map = new HashMap<String, String>();

		List<String> children = zk.getChildren(path, null);
		// 获取节点的值
		byte[] data = zk.getData(path, null, null);     
		System.out.println(new String(data));
		// 遍历节点
		for (String child : children) {
			StringBuilder sb = new StringBuilder();
			sb.append(path).append("/").append(child);
			System.out.println(sb.toString());
			ZKHomeWork.getChildNodeAndValue(sb.toString(), zk);
			// map.put(sb.toString(), new String(data));
		}

		return null;
	}

	/**
	 * 删除一个节点，不管有有没有任何子节点
	 */
	public static boolean rmr(String path, ZooKeeper zk) throws Exception {
		List<String> children = zk.getChildren(path, null);
		for (String child : children) {
			StringBuilder sb = new StringBuilder();
			sb.append(path).append("/").append(child);
			String nodePath = sb.toString();

			if (zk.getChildren(nodePath, null).isEmpty()) {
				zk.delete(sb.toString(), -1);
			} else {
				ZKHomeWork.rmr(nodePath, zk);
			}
		}
		System.out.println(path);
		//判断是否清空
		boolean flag = false;
		if(zk.getChildren(path, false).isEmpty()){
			zk.delete(path, -1);
			flag = true;
		}else{
			//没有清空继续执行
			ZKHomeWork.rmr(path, zk);
		}
			
		return flag;
	}

	/**
	 * 级联创建任意节点
	 */
	public static boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {
		boolean flag = false;
		String[] split = znodePath.split("/");
		// System.out.println(Arrays.toString(split));
		int size = split.length;
		//利用StringBuilder拼接字符串
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < split.length; i++) {
			String path = sb.append("/").append(split[i]).toString();
			System.out.println(path);
			if (null == (zk.exists(path, null)) ? true : false) {
				if (i == size - 1) {
					// 如果是最后一个节点，需要把data信息写进去
					zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					flag = true;
				} else {
					zk.create(path, "empty".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			}
		}
		return flag;
	}

	/**
	 * 清空子节点
	 */
	public static boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception {
		
		List<String> children = zk.getChildren(path, null);
		//遍历路径下的所有子节点
		for (String child : children) {
			StringBuilder sb = new StringBuilder();
			sb.append(path).append("/").append(child);
			String nodePath = sb.toString();
			//如果子节点下面没有子节点,则删除，有子节点则继续
			if (zk.getChildren(nodePath, null).isEmpty()) {
				zk.delete(sb.toString(), -1);
			} else {
				ZKHomeWork.rmr(nodePath, zk);
			}
		}
		//判断是否清空
		boolean flag = false;
		if (!zk.getChildren(znodePath, false).isEmpty()) {
			ZKHomeWork.rmr(path, zk);
		} else {
			flag = true;
		}
		return flag;
	}

}
