package cn.wsc.cb.cache;

import cn.wsc.cb.annotation.ThreadSafe;
import cn.wsc.cb.conf.ConfAttributes;
import cn.wsc.cb.conf.ConfManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Kali on 14-5-15.
 * Concrete buffer manager for each type of stream
 */
class SpecificBufferManager {

    private final int MAX_RAM_BUFFER_SIZE =
            Integer.parseInt(
                    ConfManager.getAttribute(
                            ConfAttributes.MAX_DATA_BUFFER_NUM));

    //the stream type of this buffer manger services for
    private final String type;

    //id generator,generator continuous id number for data buffer.
    private final AtomicLong idGen = new AtomicLong(0);

    //the id of head of full buffers
    private long head = -1;

    //tail of waiting
    // private long waitingTail = Long.MAX_VALUE;
    //private final Object waitingPillow = new Object();

    //progress table.
    private final Map<Integer, Long> progressTable = new HashMap<>();
    //private final Lock progressTableLock = new ReentrantLock();
    private final AtomicInteger keyGen = new AtomicInteger(0);
    //waiting pillow
    private final Object waitingPillow = new Object();

    //full RAM buffer manager
    private final FullRamBufferManager fullRamBufferManager;

    //free RAM buffer manager
    private final FreeRamBufferManager freeRamBufferManager;

    //disk buffer manager
    private final DiskBufferManager diskBufferManager;

    //swap thread and notify object
    private final Object pillow;
    private final BufferSwapThread swapThread;


    public SpecificBufferManager(String type) {
        this.type = type;
        freeRamBufferManager = new FreeRamBufferManager();
        fullRamBufferManager = new FullRamBufferManager();
        diskBufferManager = new DiskBufferManager();
        pillow = new Object();

        swapThread = new BufferSwapThread(
                pillow,
                fullRamBufferManager,
                freeRamBufferManager,
                diskBufferManager,
                this);
        swapThread.start();
    }

    /**
     * Register in.
     *
     * @return key number to get data
     */
    @ThreadSafe
    public int register() {
        int key = keyGen.getAndIncrement();
        synchronized (progressTable) {
            long pos = head < 0 ? 0 : head;
            progressTable.put(key, pos);
        }
        return key;
    }

    /**
     * Get a continuous increase id number
     *
     * @return id number
     */
    @ThreadSafe
    private long generatId() {
        return idGen.getAndIncrement();
    }

    /**
     * Get a free data buffer.
     *
     * @return
     */
    @ThreadSafe
    public DataBuffer getFree() {
        long id = generatId();
        DataBuffer buffer = freeRamBufferManager.get(id);
        return buffer;
    }

    /**
     * Put a full buffer in.
     *
     * @param buffer the buffer to put in
     */
    @ThreadSafe
    public void putFull(DataBuffer buffer) {
        synchronized (this) {
            if (++head != buffer.id()) {
                throw new RuntimeException("Id increase is invalid");
            }
        }
        fullRamBufferManager.put(buffer);
        synchronized (waitingPillow) {
            waitingPillow.notifyAll();
        }
        checkIsNeedSwap();
    }

    @ThreadSafe
    public DataBuffer getFull(int key) throws InterruptedException {
        Long pos;
        synchronized (progressTable) {
            pos = progressTable.get(key);
            if (pos == null) {
                throw new RuntimeException("Has not registered");
            }
        }
        while (pos > head) {
            //new data not arrive
            synchronized (waitingPillow) {
                waitingPillow.wait();
            }
        }
        DataBuffer buffer = fullRamBufferManager.getAndUpateCount(pos);
        DataBuffer ret = freeRamBufferManager.get(pos);
        if (buffer == null) {
            //buffer is in disk
            buffer = freeRamBufferManager.get(pos);
            diskBufferManager.load(buffer);
            buffer.increaseCount();
            fullRamBufferManager.put(buffer);
        }
        buffer.copyTo(ret);
        synchronized (progressTable){
            progressTable.put(key, pos + 1);
        }
        return ret;
    }

    @ThreadSafe
    public void releaseFull(DataBuffer buffer) {
        long id = buffer.id();
        fullRamBufferManager.release(id);
        freeRamBufferManager.release(buffer);
    }

    //check is need swap RAM buffer out to disk
    private void checkIsNeedSwap() {
        if (fullRamBufferManager.size() > MAX_RAM_BUFFER_SIZE) {
            //wake up swap thread to do swap work
            synchronized (pillow) {
                pillow.notifyAll();
            }
        }
    }

    @ThreadSafe
    public long lastIndex(){
        long last = head;
        synchronized (progressTable){
            for (Map.Entry<?,Long> entry : progressTable.entrySet()){
                if (entry.getValue() < last){
                    last = entry.getValue();
                }
            }
        }
        return last;
    }

    /**
     * Create a snap shoot of process.The progress may change after you get it.
     * @return
     */
    public List<Long> processSnapshoot(){
        synchronized (progressTable){
            LinkedList<Long> snapshoot = new LinkedList<>();
            int i = 0;
            for(Map.Entry<?,Long> e : progressTable.entrySet()){
                snapshoot.add(i++,e.getValue());
            }
            return Collections.unmodifiableList(snapshoot);
        }
    }
}
