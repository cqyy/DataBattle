package cn.wsc.cb.cache;

import cn.wsc.cb.annotation.ThreadSafe;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Kali on 14-5-15.
 */
class FullRamBufferManager {

    //the full buffer stay in the RAM may not be sequential,use map to find the one quickly
    private final Map<Long, DataBuffer> fullBuffers = new ConcurrentHashMap<>();

    private final AtomicInteger size = new AtomicInteger(0);

    /**
     * Get full data buffer and increase reference count.
     *
     * @param id id number of data buffer to get
     * @return DataBuffer if exist with the given id ,or null if doesn't
     */
    @ThreadSafe
    public DataBuffer getAndUpateCount(long id) {
        if (id < 0) {
            throw new IllegalArgumentException("negative id : " + id);
        }
        DataBuffer ret = null;
        synchronized (fullBuffers) {
            ret = fullBuffers.get(id);
            if (ret != null) {
                ret.increaseCount();
            }
        }
        return ret;
    }

    /**
     * Decrease the reference count of data buffer with given id number.
     * @param id
     */
    public void release(long id){
        synchronized (fullBuffers){
            DataBuffer buffer = fullBuffers.get(id);
            if (buffer != null){
                buffer.decreaseCount();
            }
        }
    }

    /**
     * Put a full data buffer in the manager
     *
     * @param buffer the buffer to put in
     * @throws java.lang.NullPointerException if buffer is null
     */
    @ThreadSafe
    public void put(DataBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException();
        }

        fullBuffers.put(buffer.id(), buffer);
        size.getAndIncrement();
    }

    /**
     * Remove the data buffer no matter if it is in use.Use this method only when you are sure it is safe.
     * @param id id number of data buffer to remove
     * @return removed data buffer of the given id,null if not data buffer exits with the given id
     */
    @ThreadSafe
    public DataBuffer removeAnyway(long id){
        DataBuffer buffer = fullBuffers.remove(id);
        if (buffer != null){
            size.getAndDecrement();
        }
        return buffer;
    }

    /**
     * Remove the data buffer if is not in use.
     * @param id id number of data buffer to remove
     * @return removed data buffer of the given id,null if no data buffer exists with the given id or it is in use
     */
    @ThreadSafe
    public DataBuffer removeIfFree(long id) {
        synchronized (fullBuffers) {
            DataBuffer buffer = fullBuffers.get(id);
            if (buffer != null && buffer.count() > 0) {
                //in use
                return null;
            }
            buffer = fullBuffers.remove(id);
            if (buffer != null) {
                size.getAndDecrement();
            }
            return buffer;
        }
    }

    /**
     * Get the current count of RAM buffers
     *
     * @return count of RAM buffers
     */
    @ThreadSafe
    public int size() {
        return size.get();
    }

    /**
     * Get a duplicate of full buffers.
     * This just creates a new current view .
     *
     * @return
     */
    @ThreadSafe
    List<DataBuffer> duplicate() {
        synchronized (fullBuffers) {
            List<DataBuffer> dup = new LinkedList<>();
            for (Map.Entry<?, DataBuffer> entry : fullBuffers.entrySet()) {
                dup.add(entry.getValue());
            }
            return dup;
        }
    }
}
