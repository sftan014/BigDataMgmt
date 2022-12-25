// A1 Task 1
// 15/10/2022
// Combine two .txt data into a new .txt file.

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;

public class solution1 {

    public static void main(String[] args) throws Exception {
        String input1 = args[0];
        String input2 = args[1];
        String output = args[2];

        Configuration conf = new Configuration();

        FileSystem input_fs1 = FileSystem.get(URI.create(input1), conf);
        FileSystem input_fs2 = FileSystem.get(URI.create(input2), conf);
        FileSystem output_fs = FileSystem.get(URI.create(output), conf);

        Path input_path1 = new Path(input1); // first .txt
        Path input_path2 = new Path(input2); // second .txt
        Path output_path = new Path(output); // combined .txt file

        FSDataInputStream in1 = input_fs1.open(input_path1); // open 1st .txt file
        FSDataInputStream in2 = input_fs2.open(input_path2); // open 2nd .txt file
        FSDataOutputStream out = output_fs.create(output_path); // create new .txt file

        IOUtils.copyBytes(in1, out, 4096, false); // false will allow sys to continue to the next line
        IOUtils.copyBytes(in2, out, 4096, true); // true will end the code after running.

        //input_fs1.delete(input_path1, true);
        //input_fs2.delete(input_path2, true);
        //output_fs.delete(output_path, true);
    }
}