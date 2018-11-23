### hadoop-hdfs-api的练习操作

**action**

​	AchieveClass.java：删除HDFS上的某个文件夹（级联删除），删除某路径下的特定类型的文件，删除HDFS集群中所有的空文件和空目录，使用流的方式上传文件、下载文件，从随机地方开始读任意长度，手动拷贝某个特定的数据块

​	CompareList.java：比较ListLocatedStatus和ListFiles的区别

​	HDFSStage.java：统计HDFS文件系统中文件大小小于默认块大小的文件占比，统计HDFS文件系统中的平均数据块数，HDFS文件系统中的平均副本数，**求给定数组轴图中的蓄水总量**（LeetCode）

**utils**

​	HDFSUtils.java：初始化FileSystem对象，关闭FileSystem连接，创建一个固定长度随机赋值的数组