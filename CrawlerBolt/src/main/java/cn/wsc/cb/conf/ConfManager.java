package cn.wsc.cb.conf;

import java.io.*;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

/**
 * <p>Manager of the configurations of the system.
 * It will get configuration values from configuration file.
 * If the configuration file doesn't exist,it will create one with default values.then exit the system.
 * You could edit the configuration file just created correctly and restart the system.</p>
 * <p>More information about configurations ,see {@link  cn.wsc.cb.conf.ConfAttributes}</p>
 * @author yy
 * @version 1.0
 */
public final class ConfManager {

    private static final String conf_dir = "./conf/";             //properties file directory
    private static final String conf_name = "crawler_bolt_conf.xml";     //properties file name

    private static Properties conf = new Properties();

        //forbid to get instance
    private ConfManager(){
        throw new RuntimeException();
    }

    //init
    static {
        try{
            File confFile = new File(conf_dir + conf_name);
            if(!confFile.exists()){
                createTemplate();
            }
            conf.loadFromXML(new FileInputStream(new File(conf_dir + conf_name)));
        }catch (FileNotFoundException e){
            System.err.println("Properties file not found !");
        } catch (InvalidPropertiesFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the value of configuration a attribute.
     * If the configuration file doesn't exist,it will create one with default value,then exit the system.
     * @param attribute attribute of system
     * @return value of this attribute
     */
    public static String getAttribute(ConfAttributes attribute){
        return conf.getProperty(attribute.getName());
    }

    /**
     * Create an template file of properties
     */
    private static void createTemplate() throws IOException {
        File file = new File(conf_dir);
        if(!file.exists()){
            file.mkdirs();
        }
        file = new File(conf_dir + conf_name);
        file.createNewFile();
        System.err.println("Configuration file not found,a template configuration file created at : " +  file.getAbsolutePath());
        Properties pro = new Properties();
        for(ConfAttributes attribute : ConfAttributes.values()){
            pro.setProperty(attribute.getName(),attribute.getValue());
        }

        FileOutputStream outputStream = new FileOutputStream(file);
        pro.storeToXML(outputStream,"default properties");
        outputStream.close();
    }
}
