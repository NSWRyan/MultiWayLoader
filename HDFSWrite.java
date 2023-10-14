import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class HDFSWrite{
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://jpw1ns1/");
            configuration.addResource(new Path("/home/trvanalyt/hdp314_jpw1_c6000_pro/hadoop/etc/hadoop/core-site.xml"));
            configuration.addResource(new Path("/home/trvanalyt/hdp314_jpw1_c6000_pro/hadoop/etc/hadoop/hdfs-site.xml"));
            FileSystem fileSystem = FileSystem.get(configuration);
            //Create a path
            Path hdfsWritePath = new Path("hdfs://jpw1ns1/user/trvanalyt_dev/test_ryan/test.txt");
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);

            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
            bufferedWriter.write("Java API to write data in HDFS");
            bufferedWriter.close();
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}