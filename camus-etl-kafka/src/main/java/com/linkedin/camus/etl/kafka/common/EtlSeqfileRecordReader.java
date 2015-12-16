package com.linkedin.camus.etl.kafka.common;

/**
 * Created by vincentye on 11/2/15.
 */
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlSeqfileSplit;
import kafka.message.Message;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;

public class EtlSeqfileRecordReader extends RecordReader<EtlKey, CamusWrapper> {
    private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
    private static final String START_TIME = "start.time";
    private static final String END_TIME = "end.time";
    private TaskAttemptContext context;

    private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;

    private long totalBytes;
    private long readBytes = 0;

    private boolean skipSchemaErrors = false;
    private MessageDecoder decoder;
    private final EtlKey key = new EtlKey();
    private CamusWrapper value;

    private int exceptionCount = 0;
    private long beginTimeStamp = 0;
    private long endTimeStamp = 0;

    private String statusMsg = "";

    EtlSeqfileSplit split;
    private static Logger log = Logger.getLogger(EtlSeqfileRecordReader.class);

    private SequenceFileRecordReader<Writable, BytesWritable> delegateReader = new SequenceFileRecordReader<Writable, BytesWritable>();
    private String topic ;

    /**
     * Record reader to fetch directly from Kafka
     *
     * @param split
     * @throws IOException
     * @throws InterruptedException
     */
    public EtlSeqfileRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        initialize(split, context);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        delegateReader.initialize(split, context);
        topic = ((EtlSeqfileSplit) split).getTopic();
        decoder = MessageDecoderFactory.createMessageDecoder(context, topic);

        // For class path debugging
        log.info("classpath: " + System.getProperty("java.class.path"));
        ClassLoader loader = EtlSeqfileRecordReader.class.getClassLoader();
        log.info("PWD: " + System.getProperty("user.dir"));
        log.info("classloader: " + loader.getClass());
        log.info("org.apache.avro.Schema: " + loader.getResource("org/apache/avro/Schema.class"));

        this.split = (EtlSeqfileSplit) split;
        this.context = context;

        if (context instanceof Mapper.Context) {
            mapperContext = (Context) context;
        }

        this.skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

        beginTimeStamp = context.getConfiguration().getLong(START_TIME, 0);
        endTimeStamp = context.getConfiguration().getLong(END_TIME, Long.MAX_VALUE);

        log.info("beginTimeStamp: " + beginTimeStamp);
        log.info("endTimeStamp: " + endTimeStamp);

        this.totalBytes = this.split.getLength();


    }

    @Override
    public synchronized void close() throws IOException {
       delegateReader.close();
    }

    private CamusWrapper getWrappedRecord(String topicName, byte[] payload) throws IOException {
        CamusWrapper r = null;
        try {
            r = decoder.decode(payload);
        } catch (Exception e) {
            if (!skipSchemaErrors) {
                throw new IOException(e);
            }
        }
        return r;
    }

    private static byte[] getBytes(BytesWritable val) {
        byte[] buffer = val.getBytes();

        /*
         * FIXME: remove the following part once the below jira is fixed
         * https://issues.apache.org/jira/browse/HADOOP-6298
         */
        long len = val.getLength();
        byte[] bytes = buffer;
        if (len < buffer.length) {
            bytes = new byte[(int) len];
            System.arraycopy(buffer, 0, bytes, 0, (int) len);
        }

        return bytes;
    }

    @Override
    public float getProgress() throws IOException {
        if (getPos() == 0) {
            return 0f;
        }

        if (getPos() >= totalBytes) {
            return 1f;
        }
        return (float) ((double) getPos() / totalBytes);
    }

    private long getPos() throws IOException {
        return readBytes;
    }

    @Override
    public EtlKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public CamusWrapper getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        Message message = null;

        try {


            while (delegateReader.nextKeyValue()) {
                try {
                    readBytes += key.getMessageSize();
                    context.progress();
                    mapperContext.getCounter("total", "event-count").increment(1);
                    byte[] bytes = getBytes(delegateReader.getCurrentValue());
                    key.clear();
                    key.set(topic, "0", 0, 0, 0, 0);


                    long tempTime = System.currentTimeMillis();
                    CamusWrapper wrapper;
                    try {
                        wrapper = getWrappedRecord(key.getTopic(), bytes);
                    } catch (Exception e) {
                        if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
                            mapperContext.write(key, new ExceptionWritable(e));
                            exceptionCount++;
                        } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
                            exceptionCount = Integer.MAX_VALUE; //Any random value
                            log.info("The same exception has occured for more than " + getMaximumDecoderExceptionsToPrint(context) + " records. All further exceptions will not be printed");
                        }
                        continue;
                    }

                    if (wrapper == null) {
                        mapperContext.write(key, new ExceptionWritable(new RuntimeException(
                                "null record")));
                        continue;
                    }

                    long timeStamp = wrapper.getTimestamp();
                    try {
                        key.setTime(timeStamp);
                        key.addAllPartitionMap(wrapper.getPartitionMap());
                    } catch (Exception e) {
                        mapperContext.write(key, new ExceptionWritable(e));
                        continue;
                    }

                    if (timeStamp < beginTimeStamp) {
                        mapperContext.getCounter("total", "skip-old").increment(1);
                        continue;
                    } else if (timeStamp > endTimeStamp) {
                        mapperContext.getCounter("total", "skip-new").increment(1);
                        continue;
                    }

                    long secondTime = System.currentTimeMillis();
                    value = wrapper;
                    long decodeTime = ((secondTime - tempTime));

                    mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);
                    return true;
                } catch (Throwable t) {
                    Exception e = new Exception(t.getLocalizedMessage(), t);
                    e.setStackTrace(t.getStackTrace());
                    mapperContext.write(key, new ExceptionWritable(e));
                    continue;
                }
            }
        }catch (EOFException ex){
            log.warn("file " + split != null ? split.getPath(): "<unknown>" + " didn't end properly.",ex);
        }
        return false;
    }

    private void closeReader() throws IOException {
       delegateReader.close();
    }


    public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
        return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
    }
}