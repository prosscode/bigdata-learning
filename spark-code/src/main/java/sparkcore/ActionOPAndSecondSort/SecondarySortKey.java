package sparkcore.ActionOPAndSecondSort;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Author: shawn pross
 * Date: 2018/04/24
 * Description: 二次排序
 */
public class SecondarySortKey implements Serializable,Ordered<SecondarySortKey> {
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }


    @Override
    public int compare(SecondarySortKey that) {
        if (this.first-that.first != 0){
            return this.first-that.first;
        }else{
            return this.second-that.second;
        }
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if(this.first<that.first){
            return true;
        }else if(this.first==that.first && this.second<that.second){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this.first>that.first){
            return true;
        }else if(this.first==that.first && this.second>that.second){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.first == that.first && this.second == that.second){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first==that.first && this.second == that.second){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if(this.first - that.first != 0){
            return this.first-that.first;
        }else{
            return this.second-that.second;
        }
    }
}