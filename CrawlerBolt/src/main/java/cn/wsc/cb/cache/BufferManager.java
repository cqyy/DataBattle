package cn.wsc.cb.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Kali on 14-5-8.
 * The buffer manager of free buffers.
 * It holds a number of cached free buffers
 */
public class BufferManager {



    private final Map<String,SpecificBufferManager> types = new ConcurrentHashMap<>();

    private static final BufferManager instance = new BufferManager();

    private BufferManager(){};

    public static BufferManager getInstance(){
        return instance;
    }

    public  boolean registerProducer(String type) throws TypeAlreadyRegisteredException {
        if (types.containsKey(type)){
            throw new TypeAlreadyRegisteredException(type);
        }
        SpecificBufferManager manager = new SpecificBufferManager(type);
        types.put(type,manager);
        return true;
    }

    public int registerConsumer(String type){
        SpecificBufferManager manager = getManager(type);
        return manager.register();
    }

    private SpecificBufferManager getManager(String type){
        SpecificBufferManager manager = types.get(type);
        if (manager == null){
            throw new IllegalArgumentException("type not found :" + type);
        }
        return manager;
    }

    public  DataBuffer getFree(String type){
         return getManager(type).getFree();
    }

    public  void putFull(String type,DataBuffer buffer){
       getManager(type).putFull(buffer);
    }

    public  DataBuffer getFull(String type,int key) throws InterruptedException {
       return getManager(type).getFull(key);
    }

    public  void releaseFull(String type,DataBuffer buffer){
        getManager(type).releaseFull(buffer);
    }
}
