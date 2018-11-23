import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.pross.utils.HDFSUtils;

/**
 * 
 * @author pross shawn
 *
 * create time：2018年3月9日
 * 
 * content： 测试listLocatedStatus和listFiles
 */
public class CompareList {
	public static void main(String[] args) throws Exception {
		HDFSUtils.initFileSystem();
		
		FileStatus[] listStatus = HDFSUtils.fs.listStatus(new Path("/"));
		System.out.println(listStatus.length);
	
		/*
		 * listLocatedStatus 遍历出指定目录下的所有的文件和文件夹
		 */
		RemoteIterator<LocatedFileStatus> listLocatedStatus = HDFSUtils.fs.listLocatedStatus(new Path("/"));
		while(listLocatedStatus.hasNext()){
			LocatedFileStatus next = listLocatedStatus.next();
			System.out.println(next.getPath());
		}
		
		System.out.println("======");
		
		/*
		 * listFiles 遍历出指定目录下的所有文件，true为递归找出
		 */
		RemoteIterator<LocatedFileStatus> listFiles = HDFSUtils.fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath());
		}
		HDFSUtils.closeFileSystem();
		
	}
}
