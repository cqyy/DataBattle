package cn.wsc.cb.cache;

import cn.wsc.cb.annotation.ThreadSafe;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Kali on 14-5-15.
 */
class FreeRamBufferManager {

    //free buffers
    private final List<DataBuffer> freeBuffers = new LinkedList<>();

    //size of free buffers
    private int size;

    /**
     * Get a empty RAM buffer.
     *
     * @param id id number of buffer to be allocated
     * @return DataBuffer with id of {@code id}
     * @throws java.lang.IllegalArgumentException if {@code id < 0}
     */
    @ThreadSafe
    public synchronized DataBuffer get(long id) {
        if (id < 0) {
            throw new IllegalArgumentException("id should not be negative,id:" + id);
        }
        DataBuffer re;

        if (size > 0) {
            re = freeBuffers.remove(0);
            re.reset(id);
            size--;
        } else
            re = new DataBuffer(id);
        return re;
    }

    /**
     * Release a no use buffer.
     *
     * @param buffer
     */
    @ThreadSafe
    public synchronized void release(DataBuffer buffer) {
        buffer.reset(-1);
        freeBuffers.add(buffer);
        size++;
    }

    @ThreadSafe
    public int size(){
        return size;
    }
}
