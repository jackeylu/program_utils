package example.pig;

import org.apache.pig.data.DataByteArray;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by jackeylyu on 2016/3/10.
 */
public class PerformanceTest {

    @Test
    public void testSplitWithDecode(){
        String input = "";
        int key_num = 100;
        for(int i = 0; i < key_num; i++){
            input += "k"+i+"=v"+i+"&";
        }
        System.out.println("PerformanceTest.testSplitWithDecode input = ["+input+"]");

        int times = 100000;
        Map<String, String> result = new HashMap<String, String>();

        System.out.println("Testing UrlParamDecoder...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++)
            UrlParamDecoder.decodeTo(input, result);
        long diff1 = System.currentTimeMillis() - start;

        System.out.println("Testing BufferProcessor");
        ArrayList<String> keyArr = new ArrayList<String>(key_num);
        ArrayList<DataByteArray> listTuple = new ArrayList<DataByteArray>(keyArr.size());
        for (int i=0; i < key_num; i++){
            keyArr.add("k"+i);
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++){
            KVBufProcessor.process(listTuple, input, keyArr, "&", "=");
            listTuple.clear();
        }
        long diff2 = System.currentTimeMillis() - start;

        keyArr.clear();
        for (int i=0; i < 10; i++){
            int k = (int) (Math.random()*key_num);
            keyArr.add("k"+k);
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++){
            KVBufProcessor.process(listTuple, input, keyArr, "&", "=");
            listTuple.clear();
        }
        long diff3 = System.currentTimeMillis() - start;

        result.clear();
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++)
            UrlParamDecoder.decodeSplit(input, result);
        long diff4 = System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        Map<DataByteArray, DataByteArray> map = new HashMap<DataByteArray, DataByteArray>();
        for (int i = 0; i < times; i++)
            KVBufProcessor.processOneScan(map, input.getBytes(), ((byte) '&'), ((byte) '='));
        long diff5 = System.currentTimeMillis() - start;

        System.out.println("Time diff decoder = " + diff1 + ", split " + diff4
                +" vs buf "+ diff2
                + ", 10 key " + diff3
                + ", all keys(improved) " + diff5);

        assertEquals(result.size(), map.size());
        for (int i = 0; i < key_num; i++) {
            String key = "k" + i;
            String value1 = result.get(key);
            String value2 = map.get(new DataByteArray(key)).toString();
            assertEquals(value1, value2);
        }
    }

    // 这个测试耗时很长，请在需要的时候打开
    @Ignore
    public void testUrlDecoderWithBufScanner(){
        Pair<Integer, Integer>[] params = new Pair[]{
                new Pair<Integer, Integer>(10, 10),
                new Pair<Integer, Integer>(10, 100),
                new Pair<Integer, Integer>(10, 1000),
                new Pair<Integer, Integer>(10, 1000),
                new Pair<Integer, Integer>(10, 10000),
                new Pair<Integer, Integer>(10, 100000),
                new Pair<Integer, Integer>(10, 1000000),
                new Pair<Integer, Integer>(100, 10),
                new Pair<Integer, Integer>(100, 100),
                new Pair<Integer, Integer>(100, 1000),
                new Pair<Integer, Integer>(100, 1000),
                new Pair<Integer, Integer>(100, 10000),
                new Pair<Integer, Integer>(100, 100000),
                new Pair<Integer, Integer>(100, 1000000),
                new Pair<Integer, Integer>(1000, 10),
                new Pair<Integer, Integer>(1000, 100),
                new Pair<Integer, Integer>(1000, 1000),
                new Pair<Integer, Integer>(1000, 1000),
                new Pair<Integer, Integer>(1000, 10000),
                new Pair<Integer, Integer>(1000, 100000),
                new Pair<Integer, Integer>(1000, 1000000),
        };

        System.out.println("key\ttimes\turl decoder\tscanner");
        for (Pair<Integer, Integer> param : params){
            for (int i = 0; i < 10; i++) {
                int key_num = param.getFirst();
                int times = param.getSecond();
                Pair<Long, Long> cost = timeit(key_num, times, true);
                System.out.println(key_num + "\t" + times + "\t"
                        + cost.getFirst() + "\t" + cost.getSecond());
            }
        }
    }

    public static Pair<Long, Long> timeit(int key_num, int times, boolean checking){
        String input = "";
        for(int i = 0; i < key_num; i++){
            input += "k"+i+"=v"+i+"&";
        }

        Map<String, String> result = new HashMap<String, String>();
        //System.out.println("Testing UrlParamDecoder...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            result.clear();
            UrlParamDecoder.decodeTo(input, result);
        }
        long decoder_t = System.currentTimeMillis() - start;
        //System.out.println("done with " + decoder_t +" millis.");

        //System.out.println("Testing buffer scanner...");
        start = System.currentTimeMillis();
        Map<DataByteArray, DataByteArray> map = new HashMap<DataByteArray, DataByteArray>();
        for (int i = 0; i < times; i++) {
            map.clear();
            KVBufProcessor.processOneScan(map, input.getBytes(), ((byte) '&'), ((byte) '='));
        }
        long scanner_t = System.currentTimeMillis() - start;
        //System.out.println("done with "+ scanner_t + " millis.");

        if (checking) {
            assertEquals(result.size(), map.size());
            for (int i = 0; i < key_num; i++) {
                String key = "k" + i;
                String value1 = result.get(key);
                String value2 = map.get(new DataByteArray(key)).toString();
                assertEquals(value1, value2);
            }
        }

        return new Pair<Long, Long>(decoder_t, scanner_t);
    }
}
