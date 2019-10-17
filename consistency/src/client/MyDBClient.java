package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;
import jdk.nashorn.internal.codegen.CompilerConstants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {

    private NodeConfig<String> nodeConfig= null;
    private Callback callback;

    public MyDBClient() throws IOException {
    }
    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback
            callback) throws IOException {

        send(isa, request);
        this.callback = callback;
    }

    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        if(callback != null) {
            this.callback.handleResponse(bytes, header);
        }
    }

}