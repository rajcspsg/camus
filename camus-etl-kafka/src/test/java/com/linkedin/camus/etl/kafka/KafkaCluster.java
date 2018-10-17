package com.linkedin.camus.etl.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class KafkaCluster {
    
    private static final Random RANDOM = new Random();
    private static final String TEMP_DIR_PREFIX = "camus-";
    
    private final EmbeddedZookeeper zookeeper;
    private final List<KafkaServerStartable> brokers;
    private final Properties props;
    
    public KafkaCluster() throws IOException {
        this(new Properties());
    }
    
    public KafkaCluster(Properties props) throws IOException {
        this(props, 1);
    }
    
    public KafkaCluster(Properties baseProperties, int numOfBrokers) throws IOException {
        
        this.zookeeper = new EmbeddedZookeeper();
        this.brokers = new ArrayList<KafkaServerStartable>();
        
        this.props = new Properties();
        
        this.props.putAll(baseProperties);
        
        StringBuilder builder = null;
        
        for (int i = 0; i < numOfBrokers; ++i) {
            if(builder != null)
                builder.append(",");
            else
                builder = new StringBuilder();
            
            int brokerPort = getAvailablePort();
            
            builder.append("localhost:");
            builder.append(brokerPort);
            
            Properties properties = new Properties();
            properties.putAll(baseProperties);
            properties.setProperty("zookeeper.connect", zookeeper.getConnection());
            properties.setProperty("broker.id", String.valueOf(i + 1));
            properties.setProperty("host.name", "localhost");
            properties.setProperty("port", Integer.toString(brokerPort));
            properties.setProperty("log.dir", getTempDir().getAbsolutePath());
            properties.setProperty("log.flush.interval.messages", String.valueOf(1));

            brokers.add(startBroker(properties));
        }
        
        this.props.put("metadata.broker.list", builder.toString());
        this.props.put("zookeeper.connect", zookeeper.getConnection());
        
    }
    
    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(this.props);
        return props;
    }
    
    public void shutdown() {
        for(KafkaServerStartable broker : brokers) {
            broker.shutdown();
        }
        zookeeper.shutdown();
    }
    
    private static KafkaServerStartable startBroker(Properties props) {
        KafkaServerStartable server = KafkaServerStartable.fromProps(props);
        server.startup();
        return server;
    }
    

    
    private static class EmbeddedZookeeper {
        
        private final int port;
        private final File snapshotDir;
        private final File logDir;
        private final NIOServerCnxnFactory factory;

        /**
         * Constructs an embedded Zookeeper instance.
         * 
         * @param connectString Zookeeper connection string.
         * 
         * @throws IOException if an error occurs during Zookeeper initialization.
         */
        public EmbeddedZookeeper() throws IOException {
            this.port = getAvailablePort();
            this.snapshotDir = getTempDir();
            this.logDir = getTempDir();
            this.factory = new NIOServerCnxnFactory();
            //this.factory = new NIOServerCnxn.Factory(new InetSocketAddress("localhost", port), 1024);
            
            try {
                int tickTime = 500;
                factory.configure(new InetSocketAddress("localhost", port), 1024);
                factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        /**
         * Shuts down the embedded Zookeeper instance.
         */ 
        public void shutdown() {
            try {
                factory.shutdown();
                Utils.delete(snapshotDir);
                Utils.delete(logDir);
            } catch(Exception e) {

            }

        }
        
        public String getConnection() {
            return "localhost:" + port;
        }
        
    }
    
    private static File getTempDir() {
        File file = new File(System.getProperty("java.io.tmpdir"), TEMP_DIR_PREFIX + RANDOM.nextInt(10000000));
        if (!file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }
    
    private static int getAvailablePort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        try {
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }

}
