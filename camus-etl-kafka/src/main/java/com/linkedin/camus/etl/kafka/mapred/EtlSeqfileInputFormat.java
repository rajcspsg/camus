package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlSeqfileRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vincentye on 11/3/15.
 */
public class EtlSeqfileInputFormat extends FileInputFormat<EtlKey, CamusWrapper> {

    private static final Log LOG = LogFactory.getLog(EtlSeqfileInputFormat.class);

    private static final double SPLIT_SLOP = 1.1;   // 10% slop
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        CamusJob.startTiming("getSplits");
        List<InputSplit> splits = super.getSplits(job);
        List<InputSplit> ret = new ArrayList<InputSplit>();

        Path[] inPaths = getInputPaths(job);
        Map<String, String> topicsMap = new HashMap<String, String>();

        if (inPaths.length > 1) {
            StringBuilder topicRegex = new StringBuilder();
            topicRegex.append("^(")
                    .append(inPaths[0].getParent().toUri().getPath())
                    .append("/(")
                    .append(inPaths[0].getName())
                    .append(")");
            for (int i = 1; i < inPaths.length; i++) {
                Path path = inPaths[i];
                topicRegex.append("|")
                        .append(path.getParent().toUri().getPath())
                        .append("/(")
                        .append(path.getName())
                        .append(")");

            }
            topicRegex.append(").*$");
            Pattern topicPattern = Pattern.compile(topicRegex.toString());

            for (InputSplit split : splits){
                Matcher m = topicPattern.matcher(((FileSplit)split).getPath().toUri().getPath());
                if (m.find()){
                    int c = m.groupCount();
                    String topic = null;
                    while (topic == null && c  > 1 ){
                         topic = m.group(c--);
                    }
                    if (topic != null)
                            ret.add(new EtlSeqfileSplit((FileSplit)split, topic));
                    else throw new IOException("can't find topic for file " + ((FileSplit) split).getPath() +
                    "\nTopic Regax: " + topicRegex.toString());
                } else {
                    throw new IOException("can't find topic for file " + ((FileSplit) split).getPath() +
                            "\nTopic Regax: " + topicRegex.toString());
                }
            }

        } else {
            String topic = inPaths[0].getName();
            for (InputSplit split : splits){
                ret.add(new EtlSeqfileSplit((FileSplit) split, topic));
            }
        }

        CamusJob.stopTiming("getSplits");
        CamusJob.startTiming("hadoop");
        CamusJob.setTime("hadoop_start");
        return ret;
    }

    @Override
    public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new EtlSeqfileRecordReader(split, context);
    }

    public List<FileStatus> getFilesStatus(Path p, JobContext job, PathFilter inputFilter, List<IOException> errors) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        boolean recursive = getInputDirRecursive(job);
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        FileStatus[] matches = fs.globStatus(p, inputFilter);
        if (matches == null) {
            errors.add(new IOException("Input path does not exist: " + p));
        } else if (matches.length == 0) {
            errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
        } else {
            for (FileStatus globStat: matches) {
                if (globStat.isDirectory()) {
                    RemoteIterator<LocatedFileStatus> iter =
                            fs.listLocatedStatus(globStat.getPath());
                    while (iter.hasNext()) {
                        LocatedFileStatus stat = iter.next();
                        if (inputFilter.accept(stat.getPath())) {
                            if (recursive && stat.isDirectory()) {
                                addInputPathRecursively(result, fs, stat.getPath(),
                                        inputFilter);
                            } else {
                                result.add(stat);
                            }
                        }
                    }
                } else {
                    result.add(globStat);
                }
            }
        }
        return result;
    }
}
