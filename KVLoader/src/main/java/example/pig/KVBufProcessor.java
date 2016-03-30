package example.pig;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataByteArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jackeylyu on 2016/3/8.
 */
public class KVBufProcessor {
    public static Log log = LogFactory.getLog(KVBufProcessor.class);
    private static byte keyDelimiter = ((byte) '&');
    private static byte keyValueDelimiter = ((byte) '=');
    private static int INVALID_POS = -1;


    public static void scanAndPickKeyValue(ArrayList<DataByteArray> protoTuple,
                                           String line,
                                           ArrayList<String> keyArr,
                                           String keyDelimiter,
                                           String keyValueDelimiter){
        if (keyDelimiter.getBytes().length == 1 && keyValueDelimiter.getBytes().length == 1) {
            byte firstDelimiter = keyDelimiter.getBytes()[0];
            byte secondDelimiter = keyValueDelimiter.getBytes()[0];
            Map<DataByteArray, DataByteArray> map = new HashMap<DataByteArray, DataByteArray>();
            processOneScan(map, line.getBytes(), firstDelimiter, secondDelimiter);

            int len = keyArr.size();
            for (int i = 0; i < len; i++){
                protoTuple.add(null);
            }

            for (int i = 0; i < len; i++){
                protoTuple.set(i, map.get(new DataByteArray(keyArr.get(i))));
            }
        }
        else {
            process(protoTuple, line, keyArr, keyDelimiter, keyValueDelimiter);
        }
    }


    public static void processOneScan(Map<DataByteArray, DataByteArray> map,
                                      byte[] buf,
                                      byte firstDelimiter,
                                      byte secondDelimiter) {
        int len = buf.length;
        // positions in buf, start(included), end(exclusive)
        int[] keyPos = new int[]{0,INVALID_POS};
        int[] valuePos = new int[]{INVALID_POS,INVALID_POS};
        boolean processingKey = true;

        for (int i = 0; i < len; i++){
            byte b = buf[i];
            if (b == firstDelimiter){
                if (keyPos[1] > keyPos[0] && valuePos[0] > INVALID_POS) {
                    valuePos[1] = i;
                    DataByteArray value = null;
                    if (valuePos[1] > valuePos[0])
                        value = new DataByteArray(buf, valuePos[0], valuePos[1]);
                    map.put(new DataByteArray(buf, keyPos[0], keyPos[1]), value);
                }
                valuePos[0] = INVALID_POS;
                valuePos[1] = INVALID_POS;
                keyPos[0] = i+1;
                keyPos[1] = i+1;
                processingKey = true; // mark as starting a new key
            }else if (b == secondDelimiter){
                processingKey = false; // mark as starting a new value
                keyPos[1] = i;
                valuePos[0] = i+1;
                valuePos[1] = i+1;
            }else {
                if (processingKey) {
                    keyPos[1] = i+1;
                } else {
                    valuePos[1] = i+1;
                }
            }

        }

        if (keyPos[1] > keyPos[0] &&
                keyPos[0] > INVALID_POS &&
                valuePos[0] > INVALID_POS &&
                valuePos[1] >= valuePos[0]) {
            DataByteArray value = null;
            if (valuePos[1] > valuePos[0])
                value = new DataByteArray(buf, valuePos[0], valuePos[1]);

            map.put(new DataByteArray(buf, keyPos[0], keyPos[1]), value);
        }
    }

    public static void process(ArrayList<DataByteArray> protoTuple,
                               String line,
                               ArrayList<String> keyArr,
                               String keyDelimiter,
                               String keyValueDelimiter) {
        Preconditions.checkNotNull(protoTuple, "result tuple is null.");
        Preconditions.checkNotNull(line, "input text is null.");
        Preconditions.checkNotNull(keyArr, "required keys are null.");
        Preconditions.checkNotNull(keyDelimiter, "keys' delimiter is null.");
        Preconditions.checkNotNull(keyValueDelimiter, "key-value's delimiter is null.");
        Preconditions.checkArgument(keyDelimiter.length() > 0, "keys' delimiter is empty.");
        Preconditions.checkArgument(keyValueDelimiter.length() > 0, "key-value's delimiter is empty.");
        Preconditions.checkArgument(protoTuple.isEmpty());
        Preconditions.checkArgument(keyDelimiter.equals(keyValueDelimiter) == false);

        if (log.isDebugEnabled()) {
            log.debug("line = " + line);
            log.debug("key array = " + keyArr.toString());
            log.debug("keyDelimiter = [" + keyDelimiter + "]");
            log.debug("keyValueDelimiter =  [" + keyValueDelimiter + "]");
        }

        // 1. keys to map {key : position}
        int len = keyArr.size();
        Map<String, Integer> key2pos = new HashMap<String, Integer>(len);
        for (int i = 0; i < len; i++) {
            key2pos.put(keyArr.get(i), i);
            protoTuple.add(null);
        }

        // 2. processing given line from the beginning to the end.
        String[] kvs = line.split(keyDelimiter);
        for (String kv : kvs) {
            String[] kvArr = kv.split(keyValueDelimiter);

            if (log.isDebugEnabled()) {
                log.info("split result = " + Arrays.toString(kvArr));
            }
            String key = kvArr[0];
            if (kvArr.length > 1) {
                // the `keyValueDelimiter` exists in the kv
                // NOTICE, HERE WE SHOULD NOT TO TRIM KEYS
                Integer pos = key2pos.get(key);
                if (null != pos) {
                    if (log.isDebugEnabled()) {
                        log.debug("Updating pos " + pos + ", with " + kvArr[1]);
                    }
                    protoTuple.set(pos, new DataByteArray(kvArr[1]));
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("result = " + protoTuple);
        }
    }

}
