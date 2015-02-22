package com.martinkl.warc;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import com.backtype.support.FSTestCase;
import com.google.common.io.CountingOutputStream;
import com.martinkl.warc.mapred.WARCInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import com.backtype.support.TestUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class WARCTapTest extends FSTestCase {

    public void testSimpleFlow() throws Exception {
        String warc = TestUtils.getTmpPath(fs, "warc");
        String sinkPath = TestUtils.getTmpPath(fs, "sink");
        FSDataOutputStream fsStream = fs.create(new Path(warc));
        CountingOutputStream byteStream = new CountingOutputStream(new BufferedOutputStream(fsStream));
        DataOutputStream dataStream = new DataOutputStream(byteStream);
        StringBuffer buffer = new StringBuffer();
        buffer.append("WARC/1.0\r\n");
        buffer.append("WARC-Type: warcinfo\r\n");
        buffer.append("WARC-Date: 2014-03-18T17:47:38Z\r\n");
        buffer.append("WARC-Record-ID: <urn:uuid:d9bbb325-c09f-473c-8600-1c9dbd4ec443>\r\n");
        buffer.append("Content-Length: 371\r\n");
        buffer.append("Content-Type: application/warc-fields\r\n");
        buffer.append("WARC-Filename: CC-MAIN-20140313024455-00000-ip-10-183-142-35.ec2.internal.warc.gz\r\n");
        buffer.append("\r\n");
        buffer.append("robots: classic\r\n");
        buffer.append("hostname: ip-10-183-142-35.ec2.internal\r\n");
        buffer.append("software: Nutch 1.6 (CC)/CC WarcExport 1.0\r\n");
        buffer.append("isPartOf: CC-MAIN-2014-10\r\n");
        buffer.append("operator: CommonCrawl Admin\r\n");
        buffer.append("description: Wide crawl of the web with URLs provided by Blekko for March 2014\r\n");
        buffer.append("publisher: CommonCrawl\r\n");
        buffer.append("format: WARC File Format 1.0\r\n");
        buffer.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n");
        buffer.append("\r\n");
        buffer.append("\r\n");
        buffer.append("\r\n");
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(buffer.toString().getBytes("UTF-8")));
        WARCRecord record = new WARCRecord(stream);
        record.write(dataStream);
        dataStream.close();

        Tap source = new WARCTap(new WARCScheme(warc));
        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, new WARCRecordToWARCWritable());
        Tap sink = new WARCTap(new WARCScheme(sinkPath));
        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, source).addTailSink(pipe, sink);
        new HadoopFlowConnector().connect(flowDef).complete();

        JobConf conf = new JobConf(new Configuration(), WARCTapTest.class);
        conf.setInputFormat(WARCInputFormat.class);

        FileStatus[] fileStatus = fs.listStatus(new Path(sinkPath));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        Path recordFilePath = new Path("/");
        for (Path path: paths) {
            if (path.getName().contains("seg-00000") && !path.getName().contains("part")) recordFilePath = path;
        }
        WARCFileReader reader = new WARCFileReader(conf, recordFilePath);
        WARCRecord writtenRecord = reader.read();
        assert(writtenRecord.getHeader().getRecordID().equals(record.getHeader().getRecordID()));
    }
}

class WARCRecordToWARCWritable extends BaseOperation implements Function {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        WARCRecord record = (WARCRecord) functionCall.getArguments().getObject(0);
        functionCall.getOutputCollector().add(new Tuple(new WARCWritable(record)));
    }
}