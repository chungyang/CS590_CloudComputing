package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {

    protected final MessageNIOTransport<String,String> serverMessenger;
    protected String myID;
    protected long id;
    protected Long messageID = new Long(0);
    protected PriorityQueue<Message> messageQueue;
    protected Map<String, Set<Long>> ackMap = new HashMap<>();
    protected int numberOfNodes;
    protected Map<Long, Long> messageHistory = new HashMap<>();


    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID)-ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.id = Long.valueOf(myID.replace("server", ""));

        Comparator<Message> comparator = (o1, o2) -> {
            if(o1.messageID == o2.messageID){
                return (int) (o1.serverID - o2.serverID);
            }

            return (int) (o1.messageID - o2.messageID);
        };

        messageQueue = new PriorityQueue<>(comparator);

        this.serverMessenger = new
                MessageNIOTransport<>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

        this.numberOfNodes = this.serverMessenger.getNodeConfig().getNodeIDs().size();

        for(long i = 0; i < this.numberOfNodes; i++){
            messageHistory.put(i + 1, new Long(-1));
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

        try {
            Message message;

            synchronized (ackMap){
                message = new Message(this.id, this.messageID, new String(bytes, SingleServer.DEFAULT_ENCODING));
                messageQueue.add(message);
                String messageKey = String.valueOf(message.messageID) + "." + String.valueOf(message.serverID);
                Set<Long> acks = ackMap.getOrDefault(messageKey, new HashSet<>());
                acks.add(this.id);
                ackMap.put(messageKey, acks);
                this.messageID++;
            }

            byte[] relayMessageBytes = serializeMessage(message);

            for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                if (!node.equals(myID))
                    this.serverMessenger.send(node, relayMessageBytes);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {


        Message message = deserializeBytes(bytes);

        String messageKey = String.valueOf(message.messageID) + "." + String.valueOf(message.serverID);

        //Incoming message is an ack message
        if(message.message.contains("ack")){

            synchronized (ackMap) {
                Set<Long> acks = ackMap.getOrDefault(messageKey,new HashSet<>());
                acks.add(Long.valueOf(message.message.replace("ack","")));
                ackMap.put(messageKey, acks);
                processQueueRequest();
            }

            return;
        }


        synchronized (ackMap){

            messageQueue.add(message);

            if(message.messageID >= this.messageID){
                this.messageID = message.messageID + 1;
            }

            Set<Long> acks = ackMap.getOrDefault(messageKey, new HashSet<>());
            acks.add(this.id);
            ackMap.put(messageKey, acks);

            processQueueRequest();

        }

    }
    private void sendAck(Message messageToAck){

            Message ackMessage = new Message(messageToAck.serverID, messageToAck.messageID, "ack" + this.id);
            byte[] ackMessageBytes = serializeMessage(ackMessage);

            try {
                for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                    if (!node.equals(myID))

                        this.serverMessenger.send(node, ackMessageBytes);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

    }
    private void processQueueRequest(){

        while(!messageQueue.isEmpty()){
            Message headMessage = messageQueue.peek();
            sendAck(headMessage);
            String headMessageKey = String.valueOf(headMessage.messageID) + "." + String.valueOf(headMessage.serverID);

            if(ackMap.get(headMessageKey).size() != this.numberOfNodes){
                break;
            }

            ackMap.remove(headMessageKey);

            this.processQuery(messageQueue.poll().message);

        }

    }

    private byte[] serializeMessage(Message message){

        List<Byte> bytes = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byte[] serverIDbytes = byteBuffer.putLong(0, message.serverID).array();
        addBytesToList(bytes, serverIDbytes);

        byte[] messageIDbytes = byteBuffer.putLong(0, message.messageID).array();
        addBytesToList(bytes, messageIDbytes);

        byte[] messageBytes = message.message.getBytes(Charset.forName(SingleServer.DEFAULT_ENCODING));
        addBytesToList(bytes, messageBytes);

        byte[] totalBytes = new byte[bytes.size()];

        for(int i = 0; i < totalBytes.length; i++){
            totalBytes[i] = bytes.get(i);
        }

        return totalBytes;
    }

    private void addBytesToList(List<Byte> byteList, byte[] bytes){

        for(byte b : bytes){
            byteList.add(b);
        }
    }

    private Message deserializeBytes(byte[] bytes){
        ByteBuffer byteBuffer = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, 9));
        long id = byteBuffer.getLong();
        byteBuffer = ByteBuffer.wrap(Arrays.copyOfRange(bytes,8,17));
        long messageID = byteBuffer.getLong();
        String message = "";

        try{
            message = new String(Arrays.copyOfRange(bytes, 16, bytes.length), SingleServer.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new Message(id, messageID, message);
    }

    public void close() {
        super.close();
    }

    private class Message{

        protected long serverID;
        protected long messageID;
        protected String message;

        Message(long serverID, long messageID, String message){
            this.serverID = serverID;
            this.messageID = messageID;
            this.message = message;
        }

    }


}