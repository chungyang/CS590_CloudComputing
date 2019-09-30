import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;


public class TSServer {
    private static final int PORT = 12286;
    private static ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);

    private static void start(){


        try(DatagramSocket socket = new DatagramSocket(PORT)) {


            while(true){

                byte[] pakcetBuffer = new byte[16];
                DatagramPacket packet = new DatagramPacket(pakcetBuffer, pakcetBuffer.length);
                socket.receive(packet);

                long receiveTimestamp = System.currentTimeMillis();

                InetAddress address = packet.getAddress();
                int port = packet.getPort();

                byte[] outPakcetBuffer = longsToBytes(receiveTimestamp, System.currentTimeMillis());
                DatagramPacket replyPacket = new DatagramPacket(outPakcetBuffer, outPakcetBuffer.length, address, port);

                socket.send(replyPacket);

            }
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static byte[] longsToBytes(long... values){

        int count = 0;
        for(int i = 0; i < buffer.capacity(); i += 8){
            buffer.putLong(i, values[count]);
            count++;
        }

        return buffer.array();
    }


    public static void main(String[] args){
        start();
    }
}
