package cascading.warc;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.martinkl.warc.WARCWritable;
import com.martinkl.warc.mapred.WARCInputFormat;
import com.martinkl.warc.mapred.WARCOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


import java.io.*;

public class WARCScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    private String path;

    public WARCScheme(String path) {
        this.path = path;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                               Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(WARCInputFormat.class);
        FileInputFormat.addInputPaths(conf, this.path);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                             Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(NullWritable.class); // be   explicit
        conf.setOutputValueClass(WARCWritable.class); // be explicit
        conf.setOutputFormat(WARCOutputFormat.class);
        FileOutputFormat.setOutputPath(conf, new Path(this.path));
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                              SourceCall<Object[], RecordReader> sourceCall) {

        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
                          SourceCall<Object[], RecordReader> sourceCall) throws IOException {

        LongWritable key = (LongWritable) sourceCall.getContext()[0];
        WARCWritable value = (WARCWritable) sourceCall.getContext()[1];

        boolean result = sourceCall.getInput().next(key, value);

        if (!result || value == null || value.getRecord() == null)
            return false;
        sourceCall.getIncomingEntry().setTuple(new Tuple(value.getRecord()));
        return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess,
                     SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
        OutputCollector outputCollector = sinkCall.getOutput();
        WARCWritable value = (WARCWritable)tuple.getObject(0);
        outputCollector.collect(NullWritable.get(), value);
    }

    public String getIdentifier() {
        return this.path;
    }
}

