import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.regex.Pattern;

public class Server {
    private static DatagramSocket socket;
    private static int port, timeout;

    private static HashMap<String, String> table = new HashMap<>();

    private static final long maxRequests = 10;

    private class Advertiser implements Runnable {
        private MulticastSocket msocket;
        private InetAddress mgroup;
        private int mport, timeout;

        private String serverAddress;
        private int serverPort;

        @Override
        public void run() {
            String message = "UDP-MULTICAST-BRUNO " + serverAddress + " " + serverPort;
            byte[] bytes = message.getBytes();
            DatagramPacket packet = new DatagramPacket(bytes, bytes.length, mgroup, mport);
            try {
                msocket.send(packet);
            } catch (IOException e) {
                System.out.println("Failed to advertise on " + serverAddress + ":" + serverPort);
                e.printStackTrace();
            }
        }

        public Advertiser(String mhost, int mport, String serverAddress, int serverPort) throws IOException {
            this.mgroup = InetAddress.getByName(mhost);
            this.mport = mport;
            this.serverAddress = serverAddress;
            this.serverPort = serverPort;

            this.msocket = new MulticastSocket(mport);
            this.msocket.joinGroup(this.mgroup);

            this.msocket.setTimeToLive(1);
        }

        public void close() throws IOException {
            this.msocket.leaveGroup(this.mgroup);
            this.msocket.close();
        }
    }

    private static void usage() {
        System.out.println("usage: java Server PORT [TIMEOUT]");
        System.exit(0);
    }

    private static boolean checkPlateFormat(String plate) {
        System.out.println("Plate: " + plate);
        return Pattern.matches("\\w{2}-\\w{2}-\\w{2}", plate);
    }

    private static boolean checkOwnerFormat(String owner) {
        System.out.println("Owner: " + owner + "(" + owner.length() + ")");
        return owner.length() <= 256;
    }

    private static void parseArgs(String[] args) {
        if (args.length == 0) usage();

        port = Integer.parseInt(args[0]);
        timeout = args.length > 1 ? Integer.parseInt(args[1]) * 1000 : 3000;
    }

    private static String handle(String request) {
        String[] words = request.split("\\s+");

        if (words.length < 2) return "-1";

        String action = words[0].toLowerCase();

        String plate, owner;

        switch (action) {
            case "register":
                if (words.length < 3) return "-1";
                plate = words[1];
                owner = words[2];
                if (!checkPlateFormat(plate)) return "-2";
                if (!checkOwnerFormat(owner)) return "-3";

                table.put(plate, owner);
                return table.size() + "\n" + plate + " " + owner + "\n";
            case "lookup":
                plate = words[1];
                if (!checkPlateFormat(plate)) return "-4";

                owner = table.get(plate);
                if (owner == null) {
                    for (HashMap.Entry<String, String> entry : table.entrySet()) {
                        System.out.println(entry);
                    }
                    return "-5";
                }
                return table.size() + "\n" + plate + " " + owner + "\n";
            default:
                return "-6";
        }
    }

    public static void main(String[] args) throws IOException {
        parseArgs(args);

        long count = 0;

        try {
            socket = new DatagramSocket(port);
            socket.setSoTimeout(timeout);

            System.out.printf("Serving on localhost:%d with timeout %d\n", port, timeout / 1000);

            do {
                byte[] rbuf = new byte[512];
                DatagramPacket inpacket = new DatagramPacket(rbuf, rbuf.length);
                socket.receive(inpacket);

                String request = new String(inpacket.getData()).split("\0")[0];
                System.out.println("[SERVER] Received: " + request);

                String answer = handle(request);
                byte[] sbuf = answer.getBytes();
                DatagramPacket outpacket = new DatagramPacket(sbuf, sbuf.length, inpacket.getAddress(), inpacket.getPort());
                socket.send(outpacket);

                System.out.print("[SERVER] Sent: " + answer);
            } while (count < maxRequests);
        } catch (SocketTimeoutException e) {
            System.err.println("Socket timeout error: " + e.getMessage());
        } catch (SocketException e) {
            System.err.println("Server socket error: " + e.getMessage());
        }

        socket.close();
    }
}
