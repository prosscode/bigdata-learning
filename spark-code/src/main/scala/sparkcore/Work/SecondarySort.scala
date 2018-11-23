package sparkcore.Work

/**
  * Author: shawn pross
  * Date: 2018/05/02
  * Description: 
  */
class SecondarySort(val first:Int,val second:Int) extends Ordered[SecondarySort] with Serializable {
	override def compare(that: SecondarySort): Int = {
		if(this.first-that.first!=0){
			this.first-that.first
		}else{
			this.second-that.second
		}
	}
}
