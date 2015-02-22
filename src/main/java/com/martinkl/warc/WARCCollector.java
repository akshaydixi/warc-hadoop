package com.martinkl.warc;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

public class WARCCollector extends TupleEntrySchemeCollector implements OutputCollector<NullWritable, WARCWritable> {

    private final JobConf conf;
    private RecordWriter writer;
    private final FlowProcess<JobConf> hadoopFlowProcess;
    private final Tap<JobConf, RecordReader, OutputCollector> tap;
    private final Reporter reporter = Reporter.NULL;

    public WARCCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) throws IOException {
        super(flowProcess, tap.getScheme());
        this.hadoopFlowProcess = flowProcess;
        this.tap = tap;
        this.conf = new JobConf(flowProcess.getConfigCopy());
        this.setOutput(this);
    }

    @Override
    public void prepare() {
        try {
            initialize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.prepare();
    }

    private void initialize() throws IOException {
        tap.sinkConfInit(hadoopFlowProcess, conf);
        OutputFormat outputFormat = conf.getOutputFormat();
        writer = outputFormat.getRecordWriter(null, conf, java.util.UUID.randomUUID().toString(), reporter);
        sinkCall.setOutput(this);
    }

    @Override
    public void close() {
        try {
            writer.close(reporter);
        } catch (IOException e) {
            throw new TapException("Exception closing WARCTapCollector", e);
        } finally {
            super.close();
        }
    }


    public void collect(NullWritable writableComparable, WARCWritable writable) throws IOException {
        if (hadoopFlowProcess instanceof HadoopFlowProcess)
            ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();
        writer.write(writableComparable, writable);
    }
}

