package example.pig;

import org.apache.pig.data.DataByteArray;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by jackeylyu on 2016/3/10.
 */
public class KVBufProcessorTest {

    @Test
    public void testSingleKeyWithoutValue() {
        String input = "a=";
        Map<DataByteArray, DataByteArray> map = new HashMap();
        KVBufProcessor.processOneScan(map,input.getBytes(), ((byte) '&'), ((byte) '='));

        for (Map.Entry<DataByteArray, DataByteArray> entry : map.entrySet()){
            System.out.println("key:" + entry.getKey());
            System.out.println("value: " +entry.getValue());
        }

        DataByteArray key = new DataByteArray(new byte[]{((byte) 'a')});
        assertEquals(1, map.size());
        assertTrue(map.containsKey(key));
        assertEquals(null, map.get(key));
    }

    @Test
    public void testSingleKeyValue() {
        String input = "a=1";
        Map<DataByteArray, DataByteArray> map = new HashMap();
        KVBufProcessor.processOneScan(map,input.getBytes(), ((byte) '&'), ((byte) '='));

        for (Map.Entry<DataByteArray, DataByteArray> entry : map.entrySet()){
            System.out.println("key:" + entry.getKey());
            System.out.println("value: " +entry.getValue());
        }


        DataByteArray key = new DataByteArray(new byte[]{((byte) 'a')});
        assertEquals(1, map.size());
        DataByteArray expectValue = new DataByteArray("1");
        DataByteArray actValue = map.get(key);
        assertEquals(expectValue, actValue);
    }

    @Test
    public void testWithDiffKeyValue(){
        String input = "a=1:b=hello world:a=0:c";

        Map<DataByteArray, DataByteArray> map = new HashMap();
        KVBufProcessor.processOneScan(map,input.getBytes(), ((byte) ':'), ((byte) '='));
        assertEquals(2, map.size());
        assertEquals("0", map.get(new DataByteArray("a")).toString());
        assertEquals("hello world", map.get(new DataByteArray("b")).toString());


        input = "a&1:b&hello world:a&0:c";
        map = new HashMap();
        KVBufProcessor.processOneScan(map,input.getBytes(), ((byte) ':'), ((byte) '&'));
        assertEquals(2, map.size());
        assertEquals("0", map.get(new DataByteArray("a")).toString());
        assertEquals("hello world", map.get(new DataByteArray("b")).toString());
    }

    @Test
    public void testMultiKeyValues() {
        String input = "a=1&b=hello world&a=0&c";
        Map<DataByteArray, DataByteArray> map = new HashMap();
        KVBufProcessor.processOneScan(map,input.getBytes(), ((byte) '&'), ((byte) '='));

        for (Map.Entry<DataByteArray, DataByteArray> entry : map.entrySet()){
            System.out.println("key:" + entry.getKey());
            System.out.println("value: " +entry.getValue());
        }

        assertEquals(2, map.size());
        assertEquals("0", map.get(new DataByteArray("a")).toString());
        assertEquals("hello world", map.get(new DataByteArray("b")).toString());
    }

    @Test
    public void testProcessWithNormalDelimiters() throws Exception {
        String input = "&a=1&b=2&c=hello world&d=12.3&e=";
        ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>();
        ArrayList<String> requiredKeys = new ArrayList<String>(Arrays.asList("c", "d", "na", "a"));
        String keyDelimiter = "&";
        String keyValueDelimiter = "=";

        KVBufProcessor.process(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));

        protoTuple.clear();
        KVBufProcessor.scanAndPickKeyValue(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));
    }

    @Test
    public void testProcessWithDuplicatedKeys() throws Exception {
        String input = "&a=1&b=2&c=hello world&d=12.3&e=&a=5";
        ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>();
        ArrayList<String> requiredKeys = new ArrayList<String>(Arrays.asList("c", "d", "na", "a"));
        String keyDelimiter = "&";
        String keyValueDelimiter = "=";

        KVBufProcessor.process(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("5"), protoTuple.get(3));

        protoTuple.clear();
        KVBufProcessor.scanAndPickKeyValue(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("5"), protoTuple.get(3));
    }

    @Test
    public void testProcessWithLostDelimiters() throws Exception {
        String input = "&a=1&b=2&c=hello world&d=12.3&e=";
        ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>();
        ArrayList<String> requiredKeys = new ArrayList<String>(Arrays.asList("c", "d", "na", "a"));
        String keyDelimiter = "not";
        String keyValueDelimiter = "=";

        KVBufProcessor.process(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(null, protoTuple.get(0));
        assertEquals(null, protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(null, protoTuple.get(3));

        protoTuple.clear();
        KVBufProcessor.scanAndPickKeyValue(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(null, protoTuple.get(0));
        assertEquals(null, protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(null, protoTuple.get(3));
    }

    @Test
    public void testProcessWithChineseCharacters() throws Exception {
        String input = "&a=1&b=2&c=hello 中文 world&d=12.3&e=";
        ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>();
        ArrayList<String> requiredKeys = new ArrayList<String>(Arrays.asList("c", "d", "na", "a"));
        String keyDelimiter = "&";
        String keyValueDelimiter = "=";

        KVBufProcessor.process(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello 中文 world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));

        protoTuple.clear();
        KVBufProcessor.scanAndPickKeyValue(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertEquals(new DataByteArray("hello 中文 world"), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));
    }

    @Test
    public void testProcessWithGBKCharacters() throws Exception {
        // 说明pig内部是不区分编码的，都是按照字节数组来处理，至于这些字节数组怎么定义和解析，是由用户自己处理
        String input = new String("&a=1&b=2&c=hello 中文 world&d=12.3&e=".getBytes(), "GBK");
        ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>();
        ArrayList<String> requiredKeys = new ArrayList<String>(Arrays.asList("c", "d", "na", "a"));
        String keyDelimiter = "&";
        String keyValueDelimiter = "=";

        KVBufProcessor.process(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertNotSame(new DataByteArray("hello 中文 world"), protoTuple.get(0));
        assertEquals(new DataByteArray(new String("hello 中文 world".getBytes(), "GBK")), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));

        protoTuple.clear();
        KVBufProcessor.scanAndPickKeyValue(protoTuple, input, requiredKeys, keyDelimiter, keyValueDelimiter);
        assertEquals(requiredKeys.size(), protoTuple.size());
        assertNotSame(new DataByteArray("hello 中文 world"), protoTuple.get(0));
        assertEquals(new DataByteArray(new String("hello 中文 world".getBytes(), "GBK")), protoTuple.get(0));
        assertEquals(new DataByteArray("12.3"), protoTuple.get(1));
        assertEquals(null, protoTuple.get(2));
        assertEquals(new DataByteArray("1"), protoTuple.get(3));

    }
}