import java.io.IOException;
import java.net.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EchoServer {
    private static MulticastSocket multicast;
    private static InetAddress group;
    private static int mport;

    private static DatagramSocket socket;

    private static String servername, echo;

    private static final int timeout = 30000;

    private static void usage() {
        System.out.println("usage:");
        System.out.println("ECHO SERVER :: java EchoServer MULTI_PORT MULTICAST FILENAME [SOCKET_PORT [ADDRESS]]");
        System.exit(0);
    }

    private static void init(String[] args) throws IOException {
        if (args.length < 3 || args.length > 5) usage();

        mport = Integer.parseInt(args[0]);
        group = InetAddress.getByName(args[1]);

        multicast = new MulticastSocket(mport);
        multicast.joinGroup(group);
        multicast.setSoTimeout(timeout);
        multicast.setTimeToLive(1);

        servername = args[2];
        Files.createDirectories(Paths.get(servername));
        echo = "[EchoServer " + servername + "] ";

        if (args.length == 3) {
            socket = new DatagramSocket();
        } else if (args.length == 4) {
            socket = new DatagramSocket(Integer.parseInt(args[3]));
        } else {
            socket = new DatagramSocket(Integer.parseInt(args[3]), InetAddress.getByName(args[4]));
        }

        socket.setSoTimeout(timeout);
    }

    private static void die() throws IOException {
        multicast.leaveGroup(group);
        multicast.close();
        socket.close();
    }

    private static void sendSocket(String message, InetAddress destAddress, int destPort) throws IOException {
        byte[] buffer = message.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destAddress, destPort);
        socket.send(packet);
    }

    private static DatagramPacket receiveSocket() throws IOException {
        byte[] buffer = new byte[4096];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);
        return packet;
    }

    private static void sendMulticast(String message) throws IOException {
        sendSocket(message, group, mport);
    }

    private static DatagramPacket receiveMulticast() throws IOException {
        byte[] buffer = new byte[4096];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        multicast.receive(packet);
        return packet;
    }

    private static void handle(String message) throws IOException {
        try {
            Path path = Paths.get(servername, message);
            Files.createFile(path);
        } catch (FileAlreadyExistsException e) {
            System.out.println(echo + "File " + message + " already exists");
        } catch (IOException e) {
            System.out.println(echo + "File IO Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        init(args);

        long count = 0;
        System.out.printf(echo + "Listening on %s:%d / timeout %ds|\n", group, mport, timeout / 1000);

        try {
            while (true) {
                DatagramPacket packet = receiveMulticast();
                String message = new String(packet.getData(), packet.getOffset(), packet.getLength());

                System.out.println(echo + "Received message " + message);
                ++count;
                handle(message);

                sendSocket(servername, packet.getAddress(), packet.getPort());
            }
        } catch (SocketTimeoutException e) {
            System.out.println(echo + "Timeout. Received " + count + " total messages|");
        } catch (IOException e) {
            System.out.println(echo + "Socket IO Exception: " + e.getMessage() + "|");
            e.printStackTrace();
        }

        die();
    }
}
