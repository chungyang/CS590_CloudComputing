package server;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {

    Cluster cluster;
    Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        String cassandraHostAddress = isa.getAddress().getHostAddress();
        this.cluster = Cluster.builder().addContactPoint(cassandraHostAddress).build();
        session = cluster.connect(keyspace);
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            log.log(Level.INFO, "{0} received message from {1}", new Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header.sndr});
            processQuery(bytes);
            this.clientMessenger.send(header.sndr, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void processQuery(byte[] bytes){

        try {
            String query = new String(bytes, this.DEFAULT_ENCODING);
            this.session.execute(query);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    protected void processQuery(String query){
        this.session.execute(query);
    }

}