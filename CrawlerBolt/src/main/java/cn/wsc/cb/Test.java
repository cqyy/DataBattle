package cn.wsc.cb;

import cn.wsc.cb.cache.BufferManager;
import cn.wsc.cb.cache.DataBuffer;
import cn.wsc.cb.cache.TypeAlreadyRegisteredException;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Kali on 14-5-15.
 */
public class Test {

    private static final String type ="test_type";

    private static class Producer extends Thread{
        private final BufferManager manager = BufferManager.getInstance();
      //  private final String type = "test_type";
        private long id = 0;

        private void register(){
            try {
                manager.registerProducer(type);
            } catch (TypeAlreadyRegisteredException e) {
                e.printStackTrace();
            }
        }

        private void fill(ByteBuffer buffer){
            buffer.clear();
            while (buffer.hasRemaining()){
                buffer.put((byte)'y');
            }
        }

        private DataBuffer load(){
            DataBuffer buffer = manager.getFree(type);
            fill(buffer.buffer());
            return buffer;
        }

        private void send(DataBuffer buffer){
            manager.putFull(type,buffer);
        }

        @Override
        public void run() {
            System.out.println("Producer started...");
            int i = 200;
            register();
            while (i-- > 0){
                DataBuffer buffer = load();
                send(buffer);
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static class Consumer extends Thread{
        private final static Random random = new Random();
        private final BufferManager manager = BufferManager.getInstance();
        private final int sleep = 10 *random.nextInt(5);
     //   private final String type = "test_type";

        @Override
        public void run() {
            System.out.println("Consumer started...");
            int key = manager.registerConsumer(type);
            while (true){
                try {
                    DataBuffer buffer = manager.getFull(type,key);
                    manager.releaseFull(type, buffer);

                    TimeUnit.MILLISECONDS.sleep(sleep);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Producer producer = new Producer();
        Consumer consumer1 = new Consumer();
        Consumer consumer2 = new Consumer();
        producer.start();
        TimeUnit.MILLISECONDS.sleep(100);
        consumer1.start();
        consumer2.start();
//        File file = new File("./tmp/1.tmp");
//        file.delete();
    }
}
