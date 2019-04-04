package dbs;

import dbs.message.Message;
import org.jetbrains.annotations.NotNull;

import java.io.IOError;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public final class PeerSocket implements Runnable {
    private DatagramSocket socket;
    private final Peer peer;
    private final LinkedBlockingDeque<DatagramPacket> queue;
    private boolean finished = false;
    private MulticastChannel mc;
    private MulticastChannel mdb;
    private MulticastChannel mdr;
    // set to true to quit after next message.

    public PeerSocket(@NotNull Peer peer, int port, @NotNull InetAddress address)
            throws IOException {
        this.peer = peer;
        this.socket = new DatagramSocket(port, address);
        this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
    }

    public PeerSocket(@NotNull Peer peer, int port) throws IOException {
        this.peer = peer;
        this.socket = new DatagramSocket(port);
        this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
    }

    public PeerSocket(@NotNull Peer peer,  MulticastChannel mc, MulticastChannel mdb, MulticastChannel mdr) throws IOException {
        this.peer = peer;
        this.socket = new DatagramSocket();
        this.queue = new LinkedBlockingDeque<>(Configuration.socketQueueCapacity);
        this.mc = mc;
        this.mdb = mdb;
        this.mdr = mdr;
    }

    /**
     * Close the socket gracefully.
     */
    private void die() {
        if (socket == null) return;
        finished = true;

        socket.close();
        socket = null;
    }

    /**
     * Send this packet to the output socket.
     *
     * @param packet The datagram packet to be sent, taken from the front of the queue.
     */
    private void send(@NotNull DatagramPacket packet) {
        try {
            socket.send(packet);
        } catch (IOException e) {
            if (socket.isClosed()) {
                throw new IOError(e);
            }

            // TODO: How to handle other exceptions?
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Add this message to the output queue, destined to a generic channel.
     *
     * @param message The message to be sent.
     */
    public void send(@NotNull Message message) {
        MulticastChannel destinationChannel;
        switch (message.getType()) {
            case PUTCHUNK:
                destinationChannel = this.mdb;
                break;
            case CHUNK:
                destinationChannel = this.mdr;
                break;
            default:
                destinationChannel = this.mc;
                break;
        }
        queue.add(message.getPacket(Long.toString(peer.getId()), destinationChannel.getPort(), destinationChannel.getAddress()));
    }

    public final int port() {
        return socket.getLocalPort();
    }

    public final InetAddress address() {
        return socket.getLocalAddress();
    }

    public final void finish() {
        this.finished = true;
    }

    /**
     * Thread pool task. Dispatches packets added by other agents the output queue through
     * the public send* methods.
     * The construction of the datagram packet is made by the agents themselves.
     * Does nothing if called once finished.
     */
    @Override
    public void run() {
        DatagramPacket packet;

        while (!finished) {
            try {
                packet = queue.poll(Configuration.socketTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                continue;
            }

            if (packet == null) continue;
            send(packet);
        }

        // TODO: die gracefully, finish sending queued messages

        die();
    }
}