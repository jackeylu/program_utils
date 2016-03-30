package example.pig;

import java.io.File;
import java.io.PrintWriter;

/**
 * Created by jackeylyu on 2016/3/10.
 */
public class TestHelper {

    public static String createTempFile(String[] lines) throws Exception {
        File tmpFile = File.createTempFile("test", ".txt");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        PrintWriter pw = new PrintWriter(tmpFile);
        for (String line : lines) {
            pw.println(line);
            System.err.println(line);
        }
        pw.close();
        tmpFile.deleteOnExit();
        return tmpFile.getAbsolutePath();
    }
}
