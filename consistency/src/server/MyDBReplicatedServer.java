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
    protected long id; //assign a numerical ID based on myID

    //The messageID monotonically increases. It acts as the logical
    //timestamp of the message
    protected long messageID;

    // Priority of a message depends on its messageID. Smaller ID has higher
    // priority. If two messages have the same ID, then sender with smaller
    // server ID takes higher priority
    protected PriorityQueue<Message> messageQueue;

    // A hashmap to keep track of acks received for a message. Message has a key
    // with format messageID.senderID
    protected Map<String, Set<Long>> ackMap = new HashMap<>();

    // To keep track of the acknowledges sent so redundant acks don't throttle the
    // network. Once the corresponding message has been processed, its entry in
    // this hashset will be removed.
    protected Set<String> ackHistory = new HashSet<>();
    protected int numberOfNodes;


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

    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

        try {
            Message message;

            //When a server receives a client request. It puts it in the priority queue
            //and sets its own acknowledgement.
            synchronized (this){
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

            //Locking the entire server instance to prevent race condition
            //on shared variables
            synchronized (this) {
                Set<Long> acks = ackMap.getOrDefault(messageKey,new HashSet<>());
                acks.add(Long.valueOf(message.message.replace("ack","")));
                ackMap.put(messageKey, acks);
                processQueueRequest();
            }

            return;
        }


        synchronized (this){

            messageQueue.add(message);
            // This if condition prevents the possibility of a server receiving a request from
            // client and "self-generating" a message with higher priority than the
            // ones that have been acknowledged.
            if(message.messageID >= this.messageID){
                this.messageID = message.messageID + 1;
            }

            Set<Long> acks = ackMap.getOrDefault(messageKey, new HashSet<>());
            acks.add(this.id);
            ackMap.put(messageKey, acks);

            processQueueRequest();

        }

    }

    /**
     * This method sends acknowledgement for a given message to all servers except itself
     * @param messageToAck
     */
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

    /**
     * This method does two things mainly. Acknowledge the message at the head of
     * priority queue and checks if all acks have been received for this message.
     * If yes, then execute the query contained in the message. It does this until
     * either the priority queue is empty or the message at the head hasn't been
     * acknowledged by all servers.
     */
    private void processQueueRequest(){

        while(!messageQueue.isEmpty()){
            Message headMessage = messageQueue.peek();
            String headMessageKey = String.valueOf(headMessage.messageID) + "." + String.valueOf(headMessage.serverID);

            if(!ackHistory.contains(headMessageKey)){
                sendAck(headMessage);
                ackHistory.add(headMessageKey);
            }

            if(ackMap.get(headMessageKey).size() != this.numberOfNodes){
                break;
            }

            ackMap.remove(headMessageKey);
            ackHistory.remove(headMessageKey);

            this.processQuery(messageQueue.poll().message);

        }

    }

    /**
     * Serialize Message into array of bytes
     * @param message
     * @return
     */
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

    /**
     * Deserialize an array of bytes into a Message
     * @param bytes
     * @return
     */
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