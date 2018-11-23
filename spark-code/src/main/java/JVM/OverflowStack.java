package JVM;

/**
 * Author: shawn pross
 * Date: 2018/04/27
 * Description:栈溢出，栈中存放变量，基本数据类型，对象引用
 */
public class OverflowStack {
    public static void main(String[] args){
        method();
    }

    public static void method(){
        method();
    }
}
