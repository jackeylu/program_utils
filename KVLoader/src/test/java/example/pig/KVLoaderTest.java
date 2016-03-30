package example.pig;

import org.apache.commons.lang.SystemUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by jackeylyu on 2016/3/8.
 */
public class KVLoaderTest {

    @Test
    public void testKVBufProcess() throws Exception {
        // This Test only work in linux, as hadoop fs's bug in windows on permission setting.
        if (SystemUtils.IS_OS_WINDOWS) {
            System.setProperty(PigConfiguration.PIG_TEMP_DIR, "D:/tmp");
        }

        String[] input = {
                "a=hello world&b=&c=&d=12.34",
                "a=hello world&b=1&c=100&d=12.34&e=e_out&f=xxx",
                "dummy"
        };

        ArrayList<DataByteArray[]> output = new ArrayList<DataByteArray[]>(3);
        output.add(new DataByteArray[] {new DataByteArray("hello world"),
                null,new DataByteArray("12.34"),null});
        output.add(new DataByteArray[] {new DataByteArray("hello world"),
                new DataByteArray("1"),new DataByteArray("12.34"),
                new DataByteArray("e_out")});
        output.add(new DataByteArray[] {null, null, null, null});

        String filename = TestHelper.createTempFile(input);
        // key a:b:d:e
        PigServer pig = new PigServer(ExecType.LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD '" + filename + "' USING com.tencent.gdata.pig.KVLoader('&', '=', 'a:b:d:e');";
        pig.registerQuery(query);
        Iterator<?> it = pig.openIterator("A");
        int tupleCount = 0;
        int i = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                if (tuple.size() > 0) {
                    DataByteArray[] expected = output.get(i);
                    for (int j = 0; j < tuple.size(); j++) {
                        assertEquals(expected[j], tuple.get(j));
                    }
                    tupleCount++;
                    i++;
                }
            }
        }
        assertEquals(3, tupleCount);
    }

    @Test
    public void testKVBufProcess2() throws Exception {
        String[] input = {
                "a=3&b=4&c=10",
                "a=3&b=4&c=dddd",
                "a=3&b=4&c=\"dddd\"",
                "a=3&b=&c=\"dddd\"",
                "a=3&b=4&c=",
                "a=3&&c=4",
                "a=3",
                "a=3&b=4.4&c=6.4",
                "a=3&b= &c=4",
                "a=4&b&c=10"
        };

        String[][] expecteds = {
                {"3","4","10"}, // a=3&b=4&c=10
                {"3","4","dddd"}, // a=3&b=4&c=dddd
                {"3","4","\"dddd\""}, //a=3&b=4&c="dddd"
                {"3",null,"\"dddd\""}, //a=3&b=&c="dddd"
                {"3","4",null}, // a=3&b=4&c=
                {"3",null,"4"},//a=3&&c=4
                {"3",null,null}, // a=3
                {"3","4.4","6.4"}, // a=3&b=4.4&c=6.4
                {"3"," ","4"}, // a=3&b= &c=4
                {"4",null,"10"}, // a=4&b&c=10

        };

        assertEquals(input.length, expecteds.length);

        ArrayList<DataByteArray[]> expectedBytes = new ArrayList<DataByteArray[]>(expecteds.length);
        for (String[] barr : expecteds){
            DataByteArray[] dbArr = new DataByteArray[barr.length];
            int i = 0;
            for (String obj : barr) {
                if (null == obj)
                    dbArr[i] = null;
                else
                    dbArr[i] = new DataByteArray(obj);
                i++;
            }
            expectedBytes.add(dbArr);
        }

        String filename = TestHelper.createTempFile(input);
        // key a:b:d:e
        PigServer pig = new PigServer(ExecType.LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD '" + filename + "' USING com.tencent.gdata.pig.KVLoader('&', '=', 'a:b:c');";
        pig.registerQuery(query);
        Iterator<?> it = pig.openIterator("A");
        int tupleCount = 0;
        int i = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                if (tuple.size() > 0) {
                    DataByteArray[] expected = expectedBytes.get(i);
                    for (int j = 0; j < tuple.size(); j++) {
                        assertEquals(expected[j], tuple.get(j));
                    }
                    tupleCount++;
                    i++;
                }
            }
        }
        assertEquals(input.length, tupleCount);
    }
}
