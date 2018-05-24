package org.pross.day01;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.pross.utils.HDFSUtils;

/**
 * @author pross shawn
 *
 *         create time：2018年3月8日
 *
 *         content：高级编程案例
 */
public class AchieveClass {
	
	public static void main(String[] args) throws Exception {
		//删除指定的后缀的文件
//		AchieveClass.deleteSuffixFile("/");
		//上传指定文件
//		AchieveClass.putFileByStream("E:/JDBCUtil.java", "/JDBCUtil.java");
		//
//		AchieveClass.readRandom();
		//
		AchieveClass.copyBlockByFile();
	}
	
	
	/**
	 * 删除HDFS上的某个文件夹(级联删除)
	 * 
	 * @throws Exception
	 */
	public void deleteDir() throws Exception {
		HDFSUtils.initFileSystem();
		HDFSUtils.fs.delete(new Path("/wc"), true); 
		HDFSUtils.closeFileSystem();
	}

	/**
	 * 删除某个路径下特定类型的文件，比如class类型文件，比如txt类型文件
	 * @param path  HDFS上的文件路径
	 * @param suffix  文件后缀
	 * @throws Exception
	 */
	public static void deleteSuffixFile(String path) throws Exception {
		HDFSUtils.initFileSystem();
		//遍历出该目录下所有的文件和文件夹
		RemoteIterator<LocatedFileStatus> listFiles = HDFSUtils.fs.listFiles(new Path(path), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			Path filePath=next.getPath();
			String fileName=next.getPath().getName();
			
			int len=fileName.length();
//			System.out.println(fileName.substring(len-3,len));
			
			if(fileName.substring(len-4,len).equals("java")){
				HDFSUtils.fs.delete(filePath,true);
				System.out.println("delete successfully");
			}
			
		}
		HDFSUtils.closeFileSystem();
	}

	/**
	 * 删除HDFS集群中的所有空文件和空目录
	 * 
	 * @throws Exception
	 */
	public static void deleteEmptyDir(Path path) throws Exception {
		HDFSUtils.initFileSystem();
		// 当前路径就是空文件夹时
		FileStatus[] listFile = HDFSUtils.fs.listStatus(path);
		if (listFile.length == 0) {
			//删除
			HDFSUtils.fs.delete(path, true);
			return;
		}

		//不是空文件，先获取指定目录下的文件和文件夹
		RemoteIterator<LocatedFileStatus> listLocatedStatus = HDFSUtils.fs.listLocatedStatus(path);

		while (listLocatedStatus.hasNext()) {
			LocatedFileStatus next = listLocatedStatus.next();
			Path currentPath = next.getPath();
			Path parentPath=next.getPath().getParent();
			
			// 如果是文件夹，继续往下遍历
			if (next.isDirectory()) {

				// 如果是空文件夹，删除
				if (HDFSUtils.fs.listStatus(currentPath).length == 0) {
					HDFSUtils.fs.delete(currentPath, true);
					if(HDFSUtils.fs.listStatus(parentPath).length==0){
						HDFSUtils.fs.delete(parentPath, true);
					}
				} else {
					// 不是空文件夹，那么则继续遍历
					if (HDFSUtils.fs.exists(currentPath)) {
						AchieveClass.deleteEmptyDir(currentPath);
					}
				}

			// 如果是文件
			} else {
				// 获取文件的长度
				long fileLength = next.getLen();
				// 当文件是空文件时， 删除
				if (fileLength == 0) {
					HDFSUtils.fs.delete(currentPath, true);
				}
			}
			// 当空文件夹或者空文件删除时，有可能导致父文件夹为空文件夹，
			// 所以每次删除一个空文件或者空文件的时候都需要判断一下，如果真是如此，那么就需要把该文件夹也删除掉
			int length = HDFSUtils.fs.listStatus(parentPath).length;
			if(length == 0){
				HDFSUtils.fs.delete(parentPath, true);
			}
			HDFSUtils.closeFileSystem();
		}

	}
	
	/**
	 * 使用流的方式上传文件
	 * @param srcPath  上传的本地路径
	 * @param desPath  上传到HDFS上后的文件名称路径
	 * @throws Exception
	 */
	public static void putFileByStream(String srcPath,String desPath) throws Exception{
		HDFSUtils.initFileSystem();
		InputStream in = new FileInputStream(new File(srcPath));
		FSDataOutputStream out = HDFSUtils.fs.create(new Path(desPath));
		IOUtils.copyBytes(in, out,4096,true);
		System.out.println("put successfully");
		HDFSUtils.closeFileSystem();
	}
	
	/**
	 * 使用流的方式下载文件 
	 * @param srcPath HDFS上的下载文件的路径
	 * @param desPath 下载到本地的文件路径
	 * @throws Exception
	 */
	public static void getFileByStream(Path srcPath,File desPath) throws Exception{
		HDFSUtils.initFileSystem();
		FSDataInputStream in=HDFSUtils.fs.open(srcPath);
		OutputStream out=new FileOutputStream(desPath);
		IOUtils.copyBytes(in, out,4096,true);
		HDFSUtils.closeFileSystem();
	}
	
	
	/**
	 * 从随机地方开始读，读任意长度
	 * @throws Exception 
	 */
	public static void readRandom() throws Exception{
		HDFSUtils.initFileSystem();
		//先获取一个文件的输入流（HDFS）
		FSDataInputStream in=HDFSUtils.fs.open(new Path("/a.txt"));
		//将流的起始偏移量进行自定义
		in.seek(10);
		//构造一个文件的输出流(本地)
		FileOutputStream out=new FileOutputStream(new File("E:/aa.txt"));
		System.out.println("操作成功");
		HDFSUtils.closeFileSystem();
		
	}
	/**
	 * 手动拷贝某个特定的数据块（比如某个文件的第二个数据块）
	 * @throws Exception 
	 */
	public static void copyBlockByFile() throws Exception{
		HDFSUtils.initFileSystem();
		//先拿到HDFS上文件信息
		FileStatus[] listStatus = HDFSUtils.fs.listStatus(new Path("/hello.txt"));
		//获取所有block信息
		BlockLocation[] fileBlockLocations = HDFSUtils.fs.getFileBlockLocations(listStatus[0], 0L, listStatus[0].getLen());
//		System.out.println(listStatus[0]);
		
		/*
		 * 下面操作是文件有几个的block的情况，大文件分block时候需要注意偏移量
		 * 测试的文件只有一个block
		 */
		//第一个block的长度
		long length = fileBlockLocations[0].getLength();
		//第一个block的起始偏移量
		long offset = fileBlockLocations[0].getOffset();
		//System.out.println(length+"："+offset);
		
		//定义输出输出流
		FSDataInputStream in= HDFSUtils.fs.open(new Path("/hello.txt"));
		FileOutputStream out = new FileOutputStream(new File("E:/block.txt"));
		
		byte[] b=new byte[4096];
		/*
		 * 读写操作 read()参数：
		 * 		position：position in the input stream to seek
		 * 		buffer：buffer into which data is read
		 * 		offset：offset into the buffer in which data is written
		 * 		length：maximum number of bytes to read
		 */
		while(in.read(offset, b, 0, 4096)!=-1){
			out.write(b);
			offset+=4096;
			if(offset>length){
				System.out.println("操作成功");
				break;
			}
		}
		out.flush();
		out.close();
		in.close();
		HDFSUtils.closeFileSystem();
	}
	
}
