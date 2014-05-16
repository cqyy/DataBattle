package cn.wsc.cb.cache;

/**
 * Created by Kali on 14-5-15.
 */
public class TypeAlreadyRegisteredException extends Exception {
    public TypeAlreadyRegisteredException() {
    }

    public TypeAlreadyRegisteredException(String message) {
        super(message);
    }
}
