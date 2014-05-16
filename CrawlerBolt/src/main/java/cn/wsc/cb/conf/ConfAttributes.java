package cn.wsc.cb.conf;

/**
 * Configuration attributes of the system.
 * These configurations are stored in an configuration file in format of xml.
 * If you want to change these configurations,edit the configuration file.
 * <p>More information,see {@link ConfManager}</p>
 * @author yy
 * @version 1.0
 */
public enum  ConfAttributes {

    /**
     * The size of data buffer(byte).
     * <p>Attribute name in configuration file : data.buffer.size</p>
     * <p>Default value : 4096 bytes</p>
     */
    DATA_BUFFER_SIZE("data.buffer.size","4096"),


    /**
     * The temp directory for data buffer to cache in disk.
     * <p>Attribute name in configuration file : data.buffer.tmp.dir</p>
     * <p>Default value : ./ </p>
     */
    DATA_BUFFER_TMP_DIR("data.buffer.tmp.dir","./tmp/"),

   // DATA_BUFFER_TMP_GC_POINT("data.buffer.tmp.gc.poing","100M"),


    /**
     * Max number of data buffer permitted.This depends on the memory available of the machine.
     * This is not the total data buffer number permitted on the JVM,just for one type of data.
     */
    MAX_DATA_BUFFER_NUM("max_data_buffer_num","100");



    /**
     * Get the key name of this configuration attribute in the configuration file.
     * @return key name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the value of this configuration attribute.
     * @return value
     */
    public String getValue() {
        return value;
    }

    private String name;
    private String value;

    ConfAttributes(String name, String value){
        this.name = name;
        this.value = value;
    }
}
