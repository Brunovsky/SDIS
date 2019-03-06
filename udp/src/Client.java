import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class Client {
    private static DatagramSocket socket;
    private static InetAddress address;
    private static String host;
    private static int port;

    private static byte[] sbuf, rbuf;

    private static String action, plate, owner, message;

    private static void usage() {
        System.out.println("usage:");
        System.out.println("REGISTER :: java Client HOST PORT register PLATE OWNER");
        System.out.println("LOOKUP   :: java Client HOST PORT lookup PLATE");
        System.exit(0);
    }

    private static void checkPlateFormat(String plate) {
        if (!Pattern.matches("\\w{2}-\\w{2}-\\w{2}", plate)) {
            System.err.println("Plate must be like XX-XX-XX, where each X is 0-9 or A-Z");
            System.exit(0);
        }
    }

    private static void checkOwnerFormat(String owner) {
        if (owner.length() > 256) {
            System.err.println("Owner must be at most 256 characters long");
            System.exit(0);
        }
    }

    private static void parseArgs(String[] args) throws UnknownHostException {
        if (args.length < 4) usage();

        host = args[0];
        address = InetAddress.getByName(host);
        port = Integer.parseInt(args[1]);
        action = args[2].toLowerCase();

        switch (action) {
            case "register":
                if (args.length < 5) usage();

                plate = args[3].toUpperCase();
                owner = args[4].toUpperCase();

                checkPlateFormat(plate);
                checkOwnerFormat(owner);

                message = String.format("REGISTER %s %s", plate, owner);
                System.out.println("[CLIENT] " + message);
                break;
            case "lookup":
                plate = args[3].toUpperCase();

                checkPlateFormat(plate);

                message = String.format("LOOKUP %s", plate);
                System.out.println("[CLIENT] " + message);
                break;
            default:
                System.err.println("Expected 'register' or 'lookup' action");
                usage();
        }
    }

    public static void main(String[] args) throws IOException {
        parseArgs(args);

        // 1. Send packet
        socket = new DatagramSocket();
        socket.setSoTimeout(2000);

        sbuf = message.getBytes();
        DatagramPacket outpacket = new DatagramPacket(sbuf, sbuf.length, address, port);
        socket.send(outpacket);

        // Receive packet

        rbuf = new byte[512];
        DatagramPacket inpacket = new DatagramPacket(rbuf, rbuf.length);
        socket.receive(inpacket);

        String received = new String(inpacket.getData()).split("\0")[0];
        System.out.println("[CLIENT] Received: " + received);
    }
}
