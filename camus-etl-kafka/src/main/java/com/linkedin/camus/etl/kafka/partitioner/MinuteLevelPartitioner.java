package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class MinuteLevelPartitioner extends Partitioner  {
	protected  final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH/mm";
	
	 protected DateTimeFormatter outputDateFormatter = null;
	
	
	 @Override
	    public void setConf(Configuration conf)
	    {
	        if (conf != null){
	        	outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "America/Los_Angeles")));
	        }

	        super.setConf(conf);
	    }
	 
	 @Override
	    public String encodePartition(JobContext context, IEtlKey key) {
	        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
	        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());
	    }

	    @Override
	    public String generatePartitionedPath(JobContext context, String topic, String encodedPartition) {
	      StringBuilder sb = new StringBuilder();
	      sb.append(topic).append("/");
	      sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
	      DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
	      //sb.append("date_hour=");
	      sb.append(bucket.toString(outputDateFormatter));
	      
	      return sb.toString();
	    }

	    @Override
	    public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count,
	        long offset, String encodedPartition) {
	      StringBuilder sb = new StringBuilder();
	      sb.append(topic);
	      sb.append(".").append(brokerId);
	      sb.append(".").append(partitionId);
	      sb.append(".").append(count);
	      sb.append(".").append(offset);
	      sb.append(".").append(encodedPartition);
	      
	      return sb.toString();
	    }
	    
	    @Override
	    public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId,
	        String encodedPartition) {
	      StringBuilder sb = new StringBuilder();
	      sb.append("data.").append(topic.replaceAll("\\.", "_"));
	      sb.append(".").append(brokerId);
	      sb.append(".").append(partitionId);
	      sb.append(".").append(encodedPartition);
	      
	      return sb.toString();
	    }


}
