package cn.wsc.cb.cache;

import java.io.IOException;
import java.util.*;

/**
 * Created by Kali on 14-5-15.
 * Service thread for swapping data out to disk
 */
class BufferSwapThread extends Thread {

    //when nothing to do,this thread will sleep on this object
    private final Object pillow;

    //full RAM buffer manager
    private final FullRamBufferManager fullRamBufferManager;

    //free RAM buffer manager
    private final FreeRamBufferManager freeRamBufferManager;

    //disk buffer manager
    private final DiskBufferManager diskBufferManager;

    private final SpecificBufferManager specificBufferManager;

    private final int STEP = 5;

    BufferSwapThread(Object pillow,
                     FullRamBufferManager fullRamBufferManager,
                     FreeRamBufferManager freeRamBufferManager,
                     DiskBufferManager diskBufferManager,
                     SpecificBufferManager specificBufferManager) {

        this.pillow = pillow;
        this.fullRamBufferManager = fullRamBufferManager;
        this.freeRamBufferManager = freeRamBufferManager;
        this.diskBufferManager = diskBufferManager;
        this.specificBufferManager = specificBufferManager;
        // setDaemon(true);
    }


    private List<DataBuffer> sortedCopy() {
        List<DataBuffer> buffers = fullRamBufferManager.duplicate();
        Collections.sort(buffers);
        return buffers;
    }

    /**
     * recycle buffers no use.
     */
    private int recycle() {
        long last = specificBufferManager.lastIndex();
        List<DataBuffer> buffers = sortedCopy();

        int count = 0;
        //recycle buffers out of date
        for (DataBuffer buffer : buffers) {
            if (buffer.id() < last) {
                fullRamBufferManager.removeAnyway(buffer.id());
                freeRamBufferManager.release(buffer);
                count++;
            }
        }
        //System.out.println(last);
        diskBufferManager.clearTempFile(last);
        return count;
    }


    /**
     * List order not maintained
     */
    public List<Long> removeDuplicate(List<Long> arlList) {
        HashSet h = new HashSet(arlList);
        List<Long> ret = new LinkedList<>();
        ret.addAll(h);
        return ret;
    }

    /**
     * Swap some RAM buffer out
     *
     * @return
     */
    private int swap() throws IOException {
        List<DataBuffer> buffers = sortedCopy();
        List<Long> progress = removeDuplicate(specificBufferManager.processSnapshoot());
        Collections.sort(progress);
       // int step = 0;
        int count = 0;
        long checkPoint = progress.size() > 0 ? progress.remove(0) : Long.MAX_VALUE;
        long lastCheckPoing = checkPoint;
        for (DataBuffer buffer : buffers) {
            if (buffer.id() >= checkPoint) {
                lastCheckPoing = buffer.id();
                checkPoint = progress.size() > 0 ? progress.remove(0) : Long.MAX_VALUE;
                continue;
            }
            if (buffer.count() > 0) {
                lastCheckPoing = buffer.id();
                continue;
            }
            if (buffer.id() - lastCheckPoing >= STEP) {
                diskBufferManager.swap(buffer);
                fullRamBufferManager.removeIfFree(buffer.id());
                count++;
            }
        }
        return count;
    }

    @Override
    public void run() {
        System.out.println("BufferSwapThread started...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                synchronized (pillow) {
                    pillow.wait(1000);
                }
                //recycle
                recycle();
                swap();
            } catch (InterruptedException e) {
                System.out.println(e.getCause());
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
