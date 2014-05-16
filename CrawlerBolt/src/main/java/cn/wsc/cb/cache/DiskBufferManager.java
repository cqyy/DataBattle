package cn.wsc.cb.cache;

import cn.wsc.cb.annotation.ThreadSafe;
import cn.wsc.cb.conf.ConfAttributes;
import cn.wsc.cb.conf.ConfManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Kali on 14-5-15.
 */
class DiskBufferManager {

    private static final AtomicLong idGen = new AtomicLong(0);
    //temp file path in disk
    private final String tmpPath;

    //temp file suffix
    private final String suffix = ".tmp";

    //map the buffer id to name of local temp file
    private final Map<Long, String> filenames = new ConcurrentHashMap<>();


    public DiskBufferManager() {
        String path = ConfManager.getAttribute(ConfAttributes.DATA_BUFFER_TMP_DIR);
        tmpPath = path.endsWith("/") ? path : path + "/";
    }


    /**
     * Generate temp file name without repetition
     *
     * @return file name
     */
    @ThreadSafe
    private String generatUniName() {
        String name = idGen.getAndIncrement() + suffix;
        return name;
    }

    /**
     * Swap data in buffer out to local disk
     *
     * @param buffer full buffer to swap out
     * @throws java.io.IOException
     */
    @ThreadSafe
    public void swap(DataBuffer buffer) throws IOException {

        if (filenames.get(buffer.id()) != null) {
            return;
        }
        String filename = generatUniName();
        String path = tmpPath + filename;
        File file = new File(path);
        file.createNewFile();
        try {
            FileOutputStream out = new FileOutputStream(file);
            WritableByteChannel channel = out.getChannel();

            ByteBuffer bb = buffer.buffer();
            bb.flip();
            //swap out
            channel.write(bb);
            if (bb.hasRemaining()) {
                channel.write(bb);
            }
            channel.close();
            out.close();

            filenames.put(buffer.id(), filename);
        } catch (FileNotFoundException e) {
            //never happen
        }

    }

    /**
     * <p>Load buffer data from local disk to RAM.The data to load if specified
     * by the {@code id} of the {@code buffer}</p>
     * <p>The data store in the buffer will be cleared</p>
     *
     * @param buffer the container to load in
     * @throws java.lang.IllegalArgumentException if the buffer specified to load do not exists in disk
     * @throws java.lang.RuntimeException         if temp file lost in disk
     */
    @ThreadSafe
    public void load(DataBuffer buffer) {
        // System.out.println("load from disk :" + buffer.id());
        long id = buffer.id();
        String filename = filenames.get(id);
        if (filename == null) {
            //not found
            throw new IllegalArgumentException("The buffer not exits,id invalid : " + id);
        }

        String path = tmpPath + filename;
        //load data into buffer
        File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("temp file not found : " + path);
        }

        try {
            FileInputStream in = new FileInputStream(file);
            ReadableByteChannel channel = in.getChannel();
            ByteBuffer bb = buffer.buffer();
            bb.clear();

            channel.read(bb);
            while (bb.hasRemaining()) {
                channel.read(bb);
            }
            bb.flip();
            channel.close();
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            //never happen
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove temp file from disk.
     * If the temp file not found ,this will do nothing.
     *
     * @param id the number specifies the temp file
     */
    public void remove(long id) {
        String filename = filenames.get(id);
        if (filename == null) {
            return;
        }
        String path = tmpPath + filename;
        //delete local temp file
        File file = new File(path);
        if (file.exists()) {
            if (file.delete()) {
                filenames.remove(id);
            }
        }

    }

    /**
     * Clear all temp files whose id is smaller than given id.
     * Make sure the temp files is sure out of date.
     *
     * @param id
     */
    public int clearTempFile(long id) {
        int count = 0;
        for (Map.Entry<Long, String> e : filenames.entrySet()) {
            if (e.getKey() < id) {
                remove(e.getKey());
                count++;
            }
        }
        return count;
    }
}
