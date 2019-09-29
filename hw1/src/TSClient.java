import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TSClient {

    private static long remoteClock;
    private static long rtt;
    private static long offset;
    private static final int PORT = 12286;
    private static ByteBuffer outBuffer = ByteBuffer.allocate(Long.BYTES);
    private static ByteBuffer inBuffer = ByteBuffer.allocate(2 * Long.BYTES);



    private static void sync(String ip, int port){

        long current = System.currentTimeMillis();

        try (DatagramSocket socket = new DatagramSocket()) {

            InetAddress IPAddress = InetAddress.getByName(ip);
            socket.setSoTimeout(500);

            while(System.currentTimeMillis() < current + 5000) {

                long t1 = System.currentTimeMillis();
                byte[] t1InBytes = longToBytes(t1);
                DatagramPacket packet = new DatagramPacket(t1InBytes, t1InBytes.length, IPAddress, PORT);
                socket.send(packet);

                byte[] incomingByteBuffer = new byte[Long.BYTES * 2];
                packet = new DatagramPacket(incomingByteBuffer, incomingByteBuffer.length);

                try {
                    socket.receive(packet);
                    long[] replies = bytesToLong(packet.getData());

                    long t4 = System.currentTimeMillis();
                    long t2 = replies[0];
                    long t3 = replies[1];

                    rtt = t4 - t1 - (t3 - t2);
                    offset = t3 + rtt / 2 - t4;
                }
                catch(SocketTimeoutException e){
                    System.out.println("Packet sent possibly got lost");
                }

            }

            long localTime = System.currentTimeMillis();
            remoteClock = localTime + offset;


            StringBuilder sb = new StringBuilder();
            sb.append("REMOTE_TIME: ").append(remoteClock).append(System.getProperty("line.separator"))
                    .append("LOCAL_TIME: ").append(localTime).append(System.getProperty("line.separator"))
                    .append("RTT_ESTIMATE: ").append(rtt);

            System.out.println(sb.toString());


        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static byte[] longToBytes(long value){
        outBuffer.putLong(0, value);
        return outBuffer.array();
    }

    private static long[] bytesToLong(byte[] bytes){
        inBuffer.clear();
        inBuffer.put(bytes, 0, bytes.length);
        inBuffer.flip();
        long[] values = new long[bytes.length / Long.BYTES];

        for(int i = 0; i < values.length; i++){
            values[i] = inBuffer.getLong();
        }

        return values;
    }

    public static void main(String[] args){

        if(args.length < 1){
            throw new IllegalArgumentException("Need to provide remote server address");
        }

        sync(args[0], PORT);
    }
}
