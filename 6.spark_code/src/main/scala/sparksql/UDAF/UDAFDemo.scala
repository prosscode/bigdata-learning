package sparksql.UDAF

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description: 
  */
object UDAFDemo extends UserDefinedAggregateFunction{
	/**
	  * 定义输入的数据类型
	  * @return
	  */
	override def inputSchema: StructType = ???

	/**
	  * 定义输出的数据类型
	  * @return
	  */
	override def bufferSchema: StructType = ???

	/**
	  *	定义辅助字段
	  * @return
	  */
	override def dataType: DataType = ???

	/**
	  * 最后的计算的目标函数
	  * @return
	  */
	override def deterministic: Boolean = ???

	/**
	  * 初始化辅助字段
	  * @param buffer
	  */
	override def initialize(buffer: MutableAggregationBuffer): Unit = ???

	/**
	  * 更新辅助字段的值
	  * @param buffer
	  * @param input
	  */
	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

	/**
	  * 全局
	  * @param buffer1
	  * @param buffer2
	  */
	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

	override def evaluate(buffer: Row): Any = ???
}
