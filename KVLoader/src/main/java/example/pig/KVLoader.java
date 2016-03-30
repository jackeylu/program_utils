package example.pig;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by jackeylyu on 2016/3/8.
 */
public class KVLoader extends LoadFunc {
    protected RecordReader reader = null;
    private  static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static Log log = LogFactory.getLog(KVLoader.class);
    private String keyDelimiter = "&";
    private String keyValueDelimiter = "=";
    private ArrayList<String> keyArr = null;

    public KVLoader(String keys){
        // TODO(jackeylyu): what should we do if there are duplications of keys?
        String[] ks = keys.split(":");
        keyArr = new ArrayList<String>(ks.length);
        Collections.addAll(keyArr, ks);
    }

    public KVLoader(String keyDelimiter, String keyValueDelimiter,String keys){
        this(keys);
        this.keyDelimiter = keyDelimiter;
        this.keyValueDelimiter = keyValueDelimiter;
        Preconditions.checkArgument(!keyDelimiter.equals(keyValueDelimiter),
                "first and second parameters must be different.");
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        // 从reader中获取一行的数据，以bytearray形式给我们，我们用_schema文件内容将其解析
        // 返回一个tuple
        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                log.info("getNext with Done.");
                return null;
            }
            Text value = (Text) reader.getCurrentValue();
            String strValue = value.toString();
            ArrayList<DataByteArray> protoTuple = new ArrayList<DataByteArray>(keyArr.size());
            KVBufProcessor.scanAndPickKeyValue(protoTuple, strValue, keyArr, keyDelimiter, keyValueDelimiter);

            Tuple t =  mTupleFactory.newTupleNoCopy(protoTuple);
            return t;
        } catch (InterruptedException e) {
            // See org.apache.pig.PigException.determineErrorSource() for error code's range.
            // 6018 -> "Error while reading input" see https://cwiki.apache.org/confluence/display/PIG/PigErrorHandlingFunctionalSpecification
            // pig居然是用硬编码的方式处理错误码。。。
            int errCode = 6018;
            String errMsg = "Error while reading input";
            log.error(errMsg,e);
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }
}
