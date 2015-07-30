package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], Record> {
	protected DecoderFactory decoderFactory;
	protected SchemaRegistry<Schema> registry;
	private Schema latestSchema;
	private static final int PREFIX_BITS = 16;
	private ICamusAvroWrapperFactory wrapperFactory = new ReqIdCamusAvroWrapperFactory();


    /** Start of the epoch for Technorati purposes. */
private static final long TECHNORATI_EPOCH = 1380610800000L;
	
	@Override
	public void init(Properties props, String topicName) {
	    super.init(props, topicName);
	    try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class
                    .forName(
                            props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();
            
            registry.init(props);
            
            this.registry = new CachedSchemaRegistry<Schema>(registry);
            this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();

			String factoryClassName = props.getProperty("camus.message.decoder.CamusAvroWrapperFactory.class." + topicName);
			if (factoryClassName != null){
				wrapperFactory = (ICamusAvroWrapperFactory)Class.forName(factoryClassName).newInstance();
			}
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        decoderFactory = DecoderFactory.get();
	}

	private class MessageDecoderHelper {
		//private Message message;
		private ByteBuffer buffer;
		private Schema schema;
		private int start;
		private int length;
		private Schema targetSchema;
		private static final byte MAGIC_BYTE = 0x0;
		private final SchemaRegistry<Schema> registry;
		private final String topicName;
		private byte[] payload;

		public MessageDecoderHelper(SchemaRegistry<Schema> registry,
				String topicName, byte[] payload) {
			this.registry = registry;
			this.topicName = topicName;
			//this.message = message;
			this.payload = payload;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public Schema getSchema() {
			return schema;
		}

		public int getStart() {
			return start;
		}

		public int getLength() {
			return length;
		}

		public Schema getTargetSchema() {
			return targetSchema;
		}

		private ByteBuffer getByteBuffer(byte[] payload) {
			ByteBuffer buffer = ByteBuffer.wrap(payload);
			if (buffer.get() != MAGIC_BYTE)
				throw new IllegalArgumentException("Unknown magic byte!");
			return buffer;
		}

		public MessageDecoderHelper invoke() {
			buffer = getByteBuffer(payload);
			String id = Integer.toString(buffer.getInt());
			schema = registry.getSchemaByID(topicName, id);
			if (schema == null)
				throw new IllegalStateException("Unknown schema id: " + id);

			start = buffer.position() + buffer.arrayOffset();
			length = buffer.limit() - 5;

			// try to get a target schema, if any
			targetSchema = latestSchema;
			return this;
		}
	}

	public CamusWrapper<Record> decode(byte[] payload) {
		try {
			MessageDecoderHelper helper = new MessageDecoderHelper(registry,
					topicName, payload).invoke();
			DatumReader<Record> reader = (helper.getTargetSchema() == null) ? new GenericDatumReader<Record>(
					helper.getSchema()) : new GenericDatumReader<Record>(
					helper.getSchema(), helper.getTargetSchema());

			return wrapperFactory.create(reader.read(null, decoderFactory
					.binaryDecoder(helper.getBuffer().array(),
							helper.getStart(), helper.getLength(), null)));
	
		} catch (IOException e) {
			throw new MessageDecoderException(e);
		}
	}


	public static class CamusAvroWrapper extends CamusWrapper<Record> {

	    public CamusAvroWrapper(Record record) {
            super(record);
            Record header = (Record) super.getRecord().get("header");
   	        if (header != null) {
               if (header.get("server") != null) {
                   put(new Text("server"), new Text(header.get("server").toString()));
               }
               if (header.get("service") != null) {
                   put(new Text("service"), new Text(header.get("service").toString()));
               }
            }
        }
	    
	    @Override
	    public long getTimestamp() {
	        Record header = (Record) super.getRecord().get("header");

	        if (header != null && header.get("time") != null) {
	            return (Long) header.get("time");
	        } else {
	            return System.currentTimeMillis();
	        }
	    }
	    



	}

	public interface ICamusAvroWrapperFactory{
		public CamusWrapper<Record> create(Record record);
	}

	public static class CamusAvroWrapperFactory implements ICamusAvroWrapperFactory{
		public CamusWrapper<Record> create(Record record) {
			return new CamusAvroWrapper(record);
		}
	}

	public static class ReqIdCamusAvroWrapperFactory implements ICamusAvroWrapperFactory{
		public class CamusAvroWrappe extends CamusAvroWrapper{

			public CamusAvroWrappe(Record record) {
				super(record);
			}

			@Override
			public long getTimestamp() {
				if (super.getRecord().get("req_id") != null) {
					return getTimeStamp((Long) getRecord().get("req_id"));
				} else {
					return System.currentTimeMillis();
				}
			}

			private long getTimeStamp(Long req_id) {
				return (req_id >> PREFIX_BITS) + TECHNORATI_EPOCH;
			}
		}
		public CamusWrapper<Record> create(Record record) {
			return new CamusAvroWrappe(record);
		}
	}

	public static class TsCamusAvroWrapperFactory implements ICamusAvroWrapperFactory{
		public class CamusAvroWrappe extends CamusAvroWrapper{

			public CamusAvroWrappe(Record record) {
				super(record);
			}

			@Override
			public long getTimestamp() {
				if (getRecord().get("ts") != null) {
					return (Long)getRecord().get("ts");
				} else {
					return System.currentTimeMillis();
				}
			}

		}

		public CamusWrapper<Record> create(Record record) {
			return new CamusAvroWrappe(record);
		}
	}
}
