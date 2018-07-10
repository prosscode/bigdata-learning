package somecase;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Author: shawn pross
 * Date: 2018/04/24
 * Description: achieve queue feature use java programe
 */

class Add implements Runnable{
    private LinkedBlockingQueue<String> queue;

    public Add(LinkedBlockingQueue<String> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while(true){
            try {
                //先睡个5秒,方便对照数据
                Thread.currentThread().sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String value = String.valueOf(System.currentTimeMillis());
            System.out.println("add=>"+value);
            queue.add(value);
        }
    }
}
class Get implements Runnable{
    private LinkedBlockingQueue<String> queue;

    public Get(LinkedBlockingQueue<String> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while(true){
            try {
                Thread.currentThread().sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //获取并移除此队列的头元素，若队列为空，则返回null
//            System.out.println("get=>"+queue.poll());
            try {
                //获取并移除此队列头元素，若没有元素则一直阻塞。
                System.out.println("get=>"+queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

public class ThreadQueue {
    public static void main(String[] args) {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

        for (int i=0;i<2;i++){
            Add add = new Add(queue);
            Thread thread=new Thread(add,"add"+i);
            thread.start();
        }
        for(int i=0;i<2;i++){
            Get get = new Get(queue);
            Thread thread = new Thread(get, "get" + i);
            thread.start();
        }
    }
}
