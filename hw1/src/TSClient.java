import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;


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
            long counter = 0;
            while(System.currentTimeMillis() < current + 1000) {

                long t1 = System.currentTimeMillis();
                byte[] t1InBytes = longToBytes(t1);
                DatagramPacket packet = new DatagramPacket(t1InBytes, t1InBytes.length, IPAddress, port);
                socket.send(packet);

                byte[] incomingByteBuffer = new byte[Long.BYTES * 2];
                packet = new DatagramPacket(incomingByteBuffer, incomingByteBuffer.length);

                try {
                    socket.receive(packet);
                    long[] replies = bytesToLong(packet.getData());

                    long t4 = System.currentTimeMillis();
                    long t2 = replies[0];
                    long t3 = replies[1];

                    rtt += t4 - t1 - (t3 - t2);
                    long tempRtt = t4 - t1 - (t3 - t2);
                    offset += t3 + tempRtt / 2 - t4;
                    counter++;
                }
                catch(SocketTimeoutException e){
                    continue;
                }

            }

            long localTime = System.currentTimeMillis();
            remoteClock = localTime + offset / counter;
            rtt /= counter;

            StringBuilder sb = new StringBuilder();

            sb.append(String.format("%-12s %d\n", "REMOTE_TIME", remoteClock)).append(String.format("%-12s %d\n", "LOCAL_TIME", localTime)).append(String.format("%-12s %d", "RTT_ESTIMATE", rtt));

            // System.out.println(String.format("%-12s %d", "REMOTE_TIME", remoteClock));
            // System.out.println(String.format("%-12s %d", "LOCAL_TIME", localTime));
            // System.out.println(String.format("%-12s %d", "RTT_ESTIMATE", rtt));
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

        String address = args.length < 1? "localhost" : args[0];

        sync(address, PORT);
    }
}
