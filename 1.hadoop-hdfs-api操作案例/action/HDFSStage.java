
package org.pross.stageHDFS;

import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.pross.utils.HDFSUtils;

/**
 * @author pross shawn
 *
 *         create time：2018年3月12日
 *
 *         content：HDFS练习题
 */
public class HDFSStage {

	public static void main(String[] args) throws Exception {

//		 HDFSStage.lessBlockSizeOfFile();

//		 HDFSStage.avgBlockOfFile();

//		 HDFSStage.avgRepofBlock();

//		HDFSStage.countBlockOflack();

		 HDFSStage.countWater();
	}

	/**
	 * 统计出 HDFS文件系统中文件大小小于 HDFS集群中的默认块大小的文件占比 默认块大小为128MB
	 * 
	 * @throws Exception
	 */
	public static void lessBlockSizeOfFile() throws Exception {
		HDFSUtils.initFileSystem();
		FileStatus[] listStatus = HDFSUtils.fs.listStatus(new Path("/"));
		// 文件总数
		double count = listStatus.length;
		// 小于block大小的文件数个数
		double lessBlock = 0.0;

		for (int i = 0; i < count; i++) {
			if (listStatus[i].getLen() <= 134217728) {
				lessBlock += 1;
			}
		}
		System.out.println("文件总数量为：" + count + "个\n小于默认block的文件数量为：" + lessBlock + "个" + "\n文件大小小于默认块大小的文件占比:"
				+ (lessBlock / count) * 100 + "%");

		HDFSUtils.closeFileSystem();
	}

	/**
	 * 统计出 HDFS 文件系统中的平均数据块数（数据块总数/文件总数）
	 * 
	 * @throws Exception
	 */
	public static void avgBlockOfFile() throws Exception {
		HDFSUtils.initFileSystem();
		// 数据块总数
		double blockCount = 0.0;

		// 文件总数
		double fileCount = 0.0;

		RemoteIterator<LocatedFileStatus> listFiles = HDFSUtils.fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			if(next.getLen()!=0){
				fileCount++;
			}
			int blockNum = next.getBlockLocations().length;
			blockCount += blockNum;
			
		}
		System.out.println("数据块总数：" + blockCount + "\n文件数：" + fileCount + "\n平均数据块数：" + blockCount / fileCount);
		HDFSUtils.closeFileSystem();
	}

	/**
	 * HDFS文件系统中的平均副本数（副本总数/总数据块数）
	 * @throws Exception
	 */
	public static void avgRepofBlock() throws Exception {
		HDFSUtils.initFileSystem();
		// 副本总数
		double repCount = 0.0;
		// 数据块总数
		double blockCount = 0.0;

		RemoteIterator<LocatedFileStatus> listFiles = HDFSUtils.fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			int BlockNum = next.getBlockLocations().length;
			
			if (BlockNum != 0) {
				int repNum = next.getReplication();
				int oneRepCount = BlockNum * repNum;
				repCount += oneRepCount;
				blockCount += BlockNum;
			}
		}
		System.out.println("副本总数：" + repCount + "\n数据块总数：" + blockCount + "\n平均副本数：" + repCount / blockCount);
		HDFSUtils.closeFileSystem();
	}

	
	/**
	 * 统计 HDFS整个文件系统中的不足指定数据块大小的数据块的比例 指定数据块大小为128MB
	 * 
	 * @throws Exception
	 */
	public static void countBlockOflack() throws Exception {
		HDFSUtils.initFileSystem();
		// 数据块总数
		double blockCount = 0.0;
		// 满指定大小的数据块数量
		double unlackBlcok = 0.0;

		RemoteIterator<LocatedFileStatus> listFiles = HDFSUtils.fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			// 数据块的数量
			int length = next.getBlockLocations().length;
			blockCount += length;

			// 获取当前文件的大小
			long len = next.getLen();
			// 获取当前文件设置的Block的大小
			long blockSize = next.getBlockSize();

			int result = (int) ((int) len / blockSize);
			// 满blocksize大小的数量
			unlackBlcok += result;
		}

		System.out.println("总数据块：" + blockCount + "个\n不足指定数据块：" + (blockCount - unlackBlcok) + "个\n占比："
				+ (double) ((blockCount - unlackBlcok) / blockCount) * 100 + "%");
		HDFSUtils.closeFileSystem();
	}

	
	/**
	 * 统计出一个给定数组的蓄水总量
	 * 从左到右求出每个点的左边最大高度，从右到左求出每个点右边的最大高度
	 * 如果左右高度的最小值比该点高，就可以装水
	 */
	public static void countWater() {

		int[] IntArray = HDFSUtils.createIntArray(8, 9);
		System.out.println("随机的数组为："+Arrays.toString(IntArray));
		
		int len = IntArray.length;
		int max = 0;
		//左边开始
		int lArr[] = new int[len];
		for (int i = 0; i < len; i++) {
			lArr[i] = max;
			//取大值
			max = Math.max(max, IntArray[i]);
		}
		
		max = 0;
		//右边开始
		int rArr[] = new int[len];
		for (int i = len - 1; i >= 0; i--) {
			rArr[i] = max;
			max = Math.max(max, IntArray[i]);
		}
		
		//最多蓄水量
		int mostWater = 0;
		for (int i = 0; i < len; i++) {
			int min = Math.min(lArr[i], rArr[i]);
			if (min > IntArray[i]) {
				mostWater += min - IntArray[i];
			}
		}
		System.out.println("蓄水总量："+mostWater);
	}
}
