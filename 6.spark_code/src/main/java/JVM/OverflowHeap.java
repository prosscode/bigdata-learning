package JVM;

import java.util.ArrayList;

/**
 * Author: shawn pross
 * Date: 2018/04/27
 * Description: 堆溢出，Java堆中存放对象,数组
 * 不断的new对象放在list中就会出现堆溢出
 *
 */
public class OverflowHeap {
    static class method{}
    public static void main(String[] args) {
        ArrayList<Object> list = new ArrayList<>();
        while (true) {
            list.add(new OverflowHeap.method());
            System.out.println("add one.");
        }
    }
}
