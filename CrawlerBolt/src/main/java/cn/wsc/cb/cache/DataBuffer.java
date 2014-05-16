package cn.wsc.cb.cache;

import cn.wsc.cb.annotation.NotThreadSafe;
import cn.wsc.cb.annotation.ThreadSafe;
import cn.wsc.cb.conf.ConfAttributes;
import cn.wsc.cb.conf.ConfManager;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Kali on 14-5-8.
 * Buffer to store byte data.
 */
public class DataBuffer implements Comparable<DataBuffer> {

    public static final int BUFFER_SIZE =
            Integer.parseInt(
                    ConfManager.getAttribute(
                            ConfAttributes.DATA_BUFFER_SIZE));

    //buffer to store
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    //reference count.
    private final AtomicInteger count = new AtomicInteger(0);

    //id number
    private long id;

    public DataBuffer(long id) {
        this.id = id;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

     int count() {
        return count.get();
    }

    @ThreadSafe
     void increaseCount() {
        count.incrementAndGet();
    }

    @ThreadSafe
     void decreaseCount() {
        count.decrementAndGet();
    }

//    public void setCount(int count){
//        if (count < 0){
//            throw new IllegalArgumentException("count : " +count);
//        }
//        this.count.set(count);
//    }

    long id() {
        return id;
    }

    @NotThreadSafe
    void reset(long id) {
        buffer.clear();
        count.set(0);
        this.id = id;
    }

    @ThreadSafe
     synchronized void copyTo(DataBuffer buffer){
        buffer.reset(this.id);
        ByteBuffer bb = buffer.buffer;
        ByteBuffer bb1 = this.buffer;
        bb1.flip();
        bb.clear();

        while (bb1.hasRemaining()){
            bb.put(bb1.get());
        }
    }

    @Override
    public int compareTo(DataBuffer o) {
        return (int) (this.id - o.id);
    }
}
