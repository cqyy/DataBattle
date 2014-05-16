package test.cn.wsc.cb.cache;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * BufferManager Tester.
 *
 * @author <Authors name>
 * @since <pre>05/15/2014</pre>
 * @version 1.0
 */
public class BufferManagerTest extends TestCase {

//    private static class Producer extends Thread{
//        private final BufferManager manager = BufferManager.getInstance();
//        private final String type = "test_type";
//        private long id = 0;
//
//        private void register(){
//            try {
//                manager.register(type);
//            } catch (TypeAlreadyRegisteredException e) {
//                e.printStackTrace();
//            }
//        }
//
//        private void fill(ByteBuffer buffer){
//            buffer.clear();
//            while (buffer.hasRemaining()){
//                buffer.put((byte)'y');
//            }
//        }
//
//        private DataBuffer load(){
//            DataBuffer buffer = manager.getFree(type);
//            fill(buffer.buffer());
//            return buffer;
//        }
//
//        private void send(DataBuffer buffer){
//            manager.putFull(type,buffer);
//        }
//
//        @Override
//        public void run() {
//            int i = 100;
//
//            while (i > 0){
//                DataBuffer buffer = load();
//                send(buffer);
//            }
//        }
//    }
//
//
//    private static class Consumer extends Thread{
//        private final BufferManager manager = BufferManager.getInstance();
//        private final String type = "test_type";
//
//        @Override
//        public void run() {
//            long startPos = manager.getStartPos(type);
//            while (true){
//                try {
//                    DataBuffer buffer = manager.getFull(type,startPos++);
//                    System.out.println(buffer.id());
//                    manager.releaseFull(type,buffer);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }


    public BufferManagerTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testGetFree() throws Exception {

    }

    public void testGetFull() throws Exception {
        //TODO: Test goes here...
    }

    public static Test suite() {
        return new TestSuite(BufferManagerTest.class);
    }
}
