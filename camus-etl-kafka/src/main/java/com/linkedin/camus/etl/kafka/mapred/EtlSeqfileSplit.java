package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vincentye on 11/3/15.
 */
public class EtlSeqfileSplit extends FileSplit {
    private String topic;

    EtlSeqfileSplit(){}

    public EtlSeqfileSplit(Path file, long start, long length, String[] hosts, String topic) {
        super(file, start, length, hosts);
        this.topic = topic;
    }

    public EtlSeqfileSplit(FileSplit split, String topic) throws IOException {
        this(split.getPath(), split.getStart(), split.getLength(), split.getLocations(), topic);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        topic = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, topic);
    }
}
