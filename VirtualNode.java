import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class VirtualNode {
    private static final int ORACLE_TCP_PORT = 5000;
    private static final int MAX_NODES = 26;
    private static final int TUPLE_BYTES = 11;
    private static final int CONNECT_BYTES = 6;
    private static final int LSP_HEADER_BYTES = 10;
    private static final int UDP_BUFFER_SIZE = 2048;
    private static final int SEND_COPIES = 3;
    private static final long PERIODIC_LSP_MS = 15_000L;
    private static final long GRAPH_PRINT_MS = 15_000L;

    private final InetAddress oracleAddress;
    private final Inet4Address ownAddress;
    private final int ownUdpPort;
    private final DatagramSocket udpSocket;
    private final Socket tcpSocket;
    private final DataInputStream oracleInput;
    private final DataOutputStream oracleOutput;

    private final Object stateLock = new Object();
    private final Map<Character, LspRecord> knownLinkStates = new TreeMap<>();
    private volatile boolean running = true;

    private char ownNodeId = '\0';
    private List<LinkTuple> localLinkState = new ArrayList<>();
    private long localSequence = 0;
    private volatile long lastOriginatedAt = 0L;
    private volatile long lastGraphPrintAt = 0L;

    private static final class LinkTuple {
        final char nodeId;
        final Inet4Address address;
        final int udpPort;
        final int cost;

        LinkTuple(char nodeId, Inet4Address address, int udpPort, int cost) {
            this.nodeId = nodeId;
            this.address = address;
            this.udpPort = udpPort;
            this.cost = cost;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof LinkTuple)) return false;
            LinkTuple rhs = (LinkTuple) other;
            return nodeId == rhs.nodeId
                    && address.equals(rhs.address)
                    && udpPort == rhs.udpPort
                    && cost == rhs.cost;
        }

        @Override
        public int hashCode() {
            return nodeId * 31 + address.hashCode() * 17 + udpPort * 7 + cost;
        }
    }

    private static final class LspRecord {
        final char origin;
        final long sequence;
        final List<LinkTuple> tuples;
        final byte[] packet;

        LspRecord(char origin, long sequence, List<LinkTuple> tuples, byte[] packet) {
            this.origin = origin;
            this.sequence = sequence;
            this.tuples = new ArrayList<>(tuples);
            this.packet = Arrays.copyOf(packet, packet.length);
        }
    }

    private VirtualNode(InetAddress oracleAddress,
                        Inet4Address ownAddress,
                        int ownUdpPort,
                        DatagramSocket udpSocket,
                        Socket tcpSocket) throws IOException {
        this.oracleAddress = oracleAddress;
        this.ownAddress = ownAddress;
        this.ownUdpPort = ownUdpPort;
        this.udpSocket = udpSocket;
        this.tcpSocket = tcpSocket;
        this.oracleInput = new DataInputStream(tcpSocket.getInputStream());
        this.oracleOutput = new DataOutputStream(tcpSocket.getOutputStream());

        sendConnect();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: java VirtualNode <oracle-ipv4> <own-udp-port> [own-ipv4]");
            System.exit(1);
        }

        InetAddress oracle = InetAddress.getByName(args[0]);
        if (!(oracle instanceof Inet4Address) || isUnusableAddress((Inet4Address) oracle)) throw new IllegalArgumentException("Oracle address must be a reachable, non-loopback IPv4 address.");

        int udpPort = Integer.parseInt(args[1]);
        if (udpPort < 1 || udpPort > 65535) throw new IllegalArgumentException("UDP port must be between 1 and 65535.");

        Inet4Address ownAddress;
        if (args.length == 3) {
            InetAddress supplied = InetAddress.getByName(args[2]);
            if (!(supplied instanceof Inet4Address) || isUnusableAddress((Inet4Address) supplied)) throw new IllegalArgumentException("Own address must be a valid, non-loopback IPv4 address.");
            ownAddress = (Inet4Address) supplied;
        } else ownAddress = discoverLocalIPv4(oracle);

        DatagramSocket udpSocket = new DatagramSocket(null);
        udpSocket.setReuseAddress(false);
        udpSocket.bind(new InetSocketAddress(ownAddress, udpPort));
        udpSocket.setSoTimeout(500);

        Socket tcpSocket = new Socket();
        tcpSocket.connect(new InetSocketAddress(oracle, ORACLE_TCP_PORT), 5000);
        tcpSocket.setTcpNoDelay(true);

        System.out.println("VN UDP socket bound to " + ownAddress.getHostAddress() + ":" + udpPort);
        System.out.println("Connected to Oracle Node at " + oracle.getHostAddress() + ":" + ORACLE_TCP_PORT);
        new VirtualNode(oracle, ownAddress, udpPort, udpSocket, tcpSocket).run();
    }

    private static Inet4Address discoverLocalIPv4(InetAddress oracle) throws IOException {

        try (DatagramSocket probe = new DatagramSocket()) {
            probe.connect(new InetSocketAddress(oracle, 9));
            InetAddress local = probe.getLocalAddress();
            if (!(local instanceof Inet4Address) || isUnusableAddress((Inet4Address) local)) throw new IOException("Could not determine a usable local IPv4 address. Supply [own-ipv4] explicitly.");
            return (Inet4Address) local;
        }
    }

    private static boolean isUnusableAddress(Inet4Address address) {
        byte[] bytes = address.getAddress();
        int first = bytes[0] & 0xff;
        return address.isAnyLocalAddress() || address.isLoopbackAddress() || first == 127;
    }

    private void sendConnect() throws IOException {
        byte[] addressBytes = ownAddress.getAddress();
synchronized (oracleOutput) {
            oracleOutput.write(addressBytes);
            oracleOutput.writeShort(ownUdpPort);
            oracleOutput.flush();
        }
        System.out.println("Sent CONNECT: " + ownAddress.getHostAddress() + ":" + ownUdpPort);
    }

    private void run() throws IOException {
        Thread oracleReader = new Thread(this::readOracleLoop, "VN-Oracle-TCP-reader");
        oracleReader.setDaemon(true);
        oracleReader.start();

        while (running) {
            receiveUdpOnce();
            long now = System.currentTimeMillis();
            if (hasLocalState() && now - lastOriginatedAt >= PERIODIC_LSP_MS) originateLocalLsp("periodic refresh");
            if (hasLocalState() && now - lastGraphPrintAt >= GRAPH_PRINT_MS) {
                printKnownGraph();
                lastGraphPrintAt = now;
            }
        }
    }

    private void readOracleLoop() {
        try {
            while (running) {
                List<LinkTuple> tuples = readLinkStateMessage(oracleInput);
                applyOracleLinkState(tuples);
            }
        } catch (EOFException e) {
            if (running) System.err.println("Oracle Node closed the TCP connection.");
        } catch (IOException | RuntimeException e) {
            if (running) System.err.println("Error while reading LINK-STATE from Oracle Node: " + e.getMessage());
        } finally {
            running = false;
            udpSocket.close();
            try { tcpSocket.close(); } catch (IOException ignored) { }
        }
    }

    private static List<LinkTuple> readLinkStateMessage(DataInputStream input) throws IOException {

        int count = input.readUnsignedByte();
        if (count < 1 || count > MAX_NODES) throw new IOException("Invalid LINK-STATE tuple count: " + count);

        byte[] body = new byte[count * TUPLE_BYTES];
        input.readFully(body);
        ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        List<LinkTuple> tuples = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            char nodeId = (char) (buffer.get() & 0xff);
            byte[] ipBytes = new byte[4];
            buffer.get(ipBytes);
            InetAddress parsed = InetAddress.getByAddress(ipBytes);
            if (!(parsed instanceof Inet4Address) || isUnusableAddress((Inet4Address) parsed)) throw new IOException("LINK-STATE contains an invalid IPv4 address.");
            int port = buffer.getShort() & 0xffff;
            int cost = buffer.getInt();
            tuples.add(new LinkTuple(nodeId, (Inet4Address) parsed, port, cost));
        }

        validateLinkState(tuples, null);
        return tuples;
    }

    private static char validateLinkState(List<LinkTuple> tuples, Character expectedOrigin) throws IOException {
        if (tuples.isEmpty() || tuples.size() > MAX_NODES) throw new IOException("Invalid link-state tuple count.");
        boolean[] seen = new boolean[MAX_NODES];
        int zeroCostCount = 0;
        char zeroCostNode = '\0';

        for (LinkTuple tuple : tuples) {
            if (tuple.nodeId < 'A' || tuple.nodeId > 'Z') throw new IOException("Invalid node ID in link state.");
            int index = tuple.nodeId - 'A';
            if (seen[index]) throw new IOException("Duplicate node ID in link state: " + tuple.nodeId);
            seen[index] = true;
            if (tuple.udpPort < 1 || tuple.udpPort > 65535) throw new IOException("Invalid UDP port in link state.");
            if (tuple.cost == 0) {
                zeroCostCount++;
                zeroCostNode = tuple.nodeId;
            } else if (tuple.cost < 0) throw new IOException("Neighbour cost must be positive.");
        }

        if (zeroCostCount != 1) throw new IOException("A link-state message must contain exactly one zero-cost tuple.");
        if (expectedOrigin != null && zeroCostNode != expectedOrigin) throw new IOException("Zero-cost tuple does not match LSP origin.");
        return zeroCostNode;
    }

    private void applyOracleLinkState(List<LinkTuple> tuples) throws IOException {
        char detectedId = validateLinkState(tuples, null);
        byte[] packet;
        List<byte[]> previouslyKnownPackets = new ArrayList<>();
        synchronized (stateLock) {
            if (ownNodeId != '\0' && ownNodeId != detectedId) throw new IOException("Oracle changed this VN's node identity from " + ownNodeId + " to " + detectedId);
            ownNodeId = detectedId;
            localLinkState = new ArrayList<>(tuples);
            long sequence = ++localSequence;
            packet = encodeLsp(ownNodeId, sequence, localLinkState);
            knownLinkStates.put(ownNodeId,
                    new LspRecord(ownNodeId, sequence, localLinkState, packet));
            lastOriginatedAt = System.currentTimeMillis();

            for (LspRecord record : knownLinkStates.values()) {
                if (record.origin != ownNodeId) previouslyKnownPackets.add(Arrays.copyOf(record.packet, record.packet.length));
            }
        }

        System.out.println("Received LINK-STATE from Oracle Node:");
        StringBuilder line = new StringBuilder();
        for (LinkTuple tuple : tuples) {
            if (line.length() > 0) line.append(", ");
            line.append(tuple.nodeId).append('=').append(tuple.cost);
        }
        System.out.println(line);

        floodLsp(packet, null);
        for (byte[] knownPacket : previouslyKnownPackets) {
            floodLsp(knownPacket, null);
        }
    }

    private boolean hasLocalState() {
        synchronized (stateLock) {
            return ownNodeId != '\0' && !localLinkState.isEmpty();
        }
    }

    private void originateLocalLsp(String reason) throws IOException {
        byte[] packet;
        char origin;
        synchronized (stateLock) {
            if (ownNodeId == '\0' || localLinkState.isEmpty()) return;
            origin = ownNodeId;
            long sequence = ++localSequence;
            packet = encodeLsp(origin, sequence, localLinkState);
            knownLinkStates.put(origin, new LspRecord(origin, sequence, localLinkState, packet));
            lastOriginatedAt = System.currentTimeMillis();
        }
        System.out.println("Originating LSP for " + origin + " (" + reason + ").");
        floodLsp(packet, null);
    }

    private static byte[] encodeLsp(char origin, long sequence, List<LinkTuple> tuples) {
        ByteBuffer buffer = ByteBuffer.allocate(LSP_HEADER_BYTES + tuples.size() * TUPLE_BYTES)
                                      .order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte) origin);
        buffer.putLong(sequence);
        buffer.put((byte) tuples.size());
        for (LinkTuple tuple : tuples) {
            buffer.put((byte) tuple.nodeId);
            buffer.put(tuple.address.getAddress());
            buffer.putShort((short) tuple.udpPort);
            buffer.putInt(tuple.cost);
        }
        return buffer.array();
    }

    private void receiveUdpOnce() throws IOException {
        byte[] buffer = new byte[UDP_BUFFER_SIZE];
        DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
        try {
            udpSocket.receive(datagram);
            int length = datagram.getLength();
            byte[] packet = Arrays.copyOfRange(datagram.getData(), datagram.getOffset(), datagram.getOffset() + length);
            processLsp(packet, datagram.getAddress(), datagram.getPort());
        } catch (SocketTimeoutException ignored) {
        }
    }

    private void processLsp(byte[] packet, InetAddress senderAddress, int senderPort) throws IOException {
        LspRecord received = decodeLsp(packet);
        if (received.origin == ownNodeId) return;

        boolean isNew;
        synchronized (stateLock) {
            LspRecord old = knownLinkStates.get(received.origin);
            isNew = old == null || received.sequence > old.sequence;
            if (isNew) knownLinkStates.put(received.origin, received);
        }

        if (isNew) {
            System.out.println("Learned newer LSP for node " + received.origin +
                    " (sequence " + received.sequence + ").");
            floodLsp(received.packet, new InetSocketAddress(senderAddress, senderPort));
        }
    }

    private static LspRecord decodeLsp(byte[] packet) throws IOException {
        if (packet.length < LSP_HEADER_BYTES) throw new IOException("LSP packet is too short.");
        ByteBuffer buffer = ByteBuffer.wrap(packet).order(ByteOrder.BIG_ENDIAN);
        char origin = (char) (buffer.get() & 0xff);
        if (origin < 'A' || origin > 'Z') throw new IOException("Invalid LSP origin ID.");
        long sequence = buffer.getLong();
        if (sequence < 0) throw new IOException("Invalid negative LSP sequence number.");
        int count = buffer.get() & 0xff;
        if (count < 1 || count > MAX_NODES) throw new IOException("Invalid LSP tuple count.");
        if (packet.length != LSP_HEADER_BYTES + count * TUPLE_BYTES) throw new IOException("LSP packet length does not match tuple count.");

        List<LinkTuple> tuples = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            char nodeId = (char) (buffer.get() & 0xff);
            byte[] ipBytes = new byte[4];
            buffer.get(ipBytes);
            InetAddress address = InetAddress.getByAddress(ipBytes);
            if (!(address instanceof Inet4Address) || isUnusableAddress((Inet4Address) address)) throw new IOException("LSP contains an invalid IPv4 address.");
            int udpPort = buffer.getShort() & 0xffff;
            int cost = buffer.getInt();
            tuples.add(new LinkTuple(nodeId, (Inet4Address) address, udpPort, cost));
        }

        validateLinkState(tuples, origin);
        return new LspRecord(origin, sequence, tuples, packet);
    }

    private void floodLsp(byte[] packet, InetSocketAddress exclude) throws IOException {
        List<LinkTuple> neighbors = new ArrayList<>();
        synchronized (stateLock) {
            for (LinkTuple tuple : localLinkState) {
                if (tuple.cost > 0) neighbors.add(tuple);
            }
        }

        for (LinkTuple neighbor : neighbors) {
            if (exclude != null
                    && neighbor.address.equals(exclude.getAddress())
                    && neighbor.udpPort == exclude.getPort()) {
                continue;
            }
            for (int copy = 0; copy < SEND_COPIES; copy++) {
                DatagramPacket datagram = new DatagramPacket(packet, packet.length, neighbor.address, neighbor.udpPort);
                udpSocket.send(datagram);
            }
        }
    }

    private void printKnownGraph() {
        synchronized (stateLock) {
            if (ownNodeId == '\0') return;
            System.out.println("\nKnown graph at VN " + ownNodeId + ":");
            if (knownLinkStates.isEmpty()) System.out.println("  (no link-state packets known yet)");
            for (Map.Entry<Character, LspRecord> entry : knownLinkStates.entrySet()) {
                StringBuilder row = new StringBuilder("  ").append(entry.getKey()).append(": ");
                boolean first = true;
                for (LinkTuple tuple : entry.getValue().tuples) {
                    if (!first) row.append(", ");
                    row.append(tuple.nodeId).append('=').append(tuple.cost);
                    first = false;
                }
                System.out.println(row);
            }
            System.out.println();
        }
    }
}
