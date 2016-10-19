package water;

import water.fvec.Chunk;
import water.fvec.ChunkUtils;
import water.fvec.Frame;
import water.parser.BufferedString;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.UUID;

/**
 * This class can be used to read data from H2O Frames from non-H2O environments
 *
 * It is expected that the frame we want to read is already in the DKV.
 *
 * Example of use:
 * SocketChannel channel = ExternalFrameHandler.getConnection("ip:port")
 * {@code ExternalFrameReader reader = new ExternalFrameReader(channel
 * }
 */
public class ExternalFrameReader {

    private static final byte IS_NA = 1;
    private static final byte NOT_NA = 0;

    // hints for expected types in order to improve performance ( send data we actually request )
    public static final byte EXPECTED_BOOL = 0;
    public static final byte EXPECTED_BYTE = 1;
    public static final byte EXPECTED_INT = 2;
    public static final byte EXPECTED_SHORT = 3;
    public static final byte EXPECTED_LONG = 4;
    public static final byte EXPECTED_FLOAT = 5;
    public static final byte EXPECTED_DOUBLE = 6;
    public static final byte EXPECTED_STRING = 7;
    public static final byte EXPECTED_CHAR = 8;

    private AutoBuffer ab;
    private String keyName;
    private byte[] expectedTypes;
    private int chunkIdx;
    private int[] selectedColumnIndices;
    private SocketChannel channel;

    private int numOfRows;
    public ExternalFrameReader(SocketChannel channel, String keyName, byte[] expectedTypes, int chunkIdx, int[] selectedColumnIndices) throws IOException{
        this.channel = channel;
        this.keyName = keyName;
        this.expectedTypes = expectedTypes;
        this.chunkIdx = chunkIdx;
        this.selectedColumnIndices = selectedColumnIndices;
        this.ab = prepareAutoBuffer();
        this.numOfRows = ab.getInt();
    }

    private AutoBuffer prepareAutoBuffer() throws IOException{
        AutoBuffer ab = new AutoBuffer();
        ab.put1(ExternalFrameHandler.INIT_BYTE);
        ab.putInt(ExternalFrameHandler.DOWNLOAD_FRAME);
        ab.putStr(keyName);
        ab.putA1(expectedTypes);
        ab.putInt(chunkIdx);
        ab.putA4(selectedColumnIndices);
        writeToChannel(ab, channel);
        return new AutoBuffer(channel, null);
    }

    public int getNumOfRows(){
        return numOfRows;
    }

    public boolean readBool(){
        return ab.getZ();
    }
    public byte readByte(){
        return ab.get1();
    }
    public char readChar(){
        return ab.get2();
    }
    public int readInt(){
        return ab.getInt();
    }
    public float readFloat(){
        return ab.get4f();
    }
    public short readShort(){
        return ab.get2s();
    }
    public long readLong(){
        return ab.get8();
    }
    public double readDouble(){
        return ab.get8d();
    }
    public String readString(){
        return ab.getStr();
    }

    public boolean readIsNA(){
        return ab.get1() == IS_NA;
    }

    public void waitUntilAllReceived(){
        // confirm that all has been done before proceeding with the computation
        assert(ab.get1() == ExternalFrameHandler.CONFIRM_READING_DONE);
    }

    static void handleReadingFromChunk(SocketChannel sock, AutoBuffer recvAb) throws IOException {
        // buffer string to be reused for strings to avoid multiple allocation
        BufferedString valStr = new BufferedString();

        String frame_key = recvAb.getStr();
        byte[] expectedTypes = recvAb.getA1();
        assert expectedTypes != null;
        int chunk_id = recvAb.getInt();
        int[] selectedColumnIndices = recvAb.getA4();
        assert selectedColumnIndices!=null;
        Frame fr = DKV.getGet(frame_key);
        Chunk[] chunks = ChunkUtils.getChunks(fr, chunk_id);

        AutoBuffer ab = new AutoBuffer().flipForReading().clearForWriting(H2O.MAX_PRIORITY);
        ab.putInt(chunks[0]._len); // num of rows
        writeToChannel(ab, sock);

        for (int rowIdx = 0; rowIdx < chunks[0]._len; rowIdx++) { // for each row
            for(int cidx: selectedColumnIndices){ // go through the chunks
                ab.flipForReading().clearForWriting(H2O.MAX_PRIORITY); // reuse existing ByteBuffer
                // write flag weather the row is na or not
                if (chunks[cidx].isNA(rowIdx)) {
                    ab.put1(IS_NA);
                } else {
                    ab.put1(NOT_NA);

                    final Chunk chnk = chunks[cidx];
                    switch (expectedTypes[cidx]) {
                        case EXPECTED_BYTE:
                            ab.put1((byte)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_BOOL:
                            ab.put1((byte)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_CHAR:
                            ab.put2((char)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_SHORT:
                            ab.put2s((short)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_FLOAT:
                            ab.put4f((float)chnk.atd(rowIdx));
                            break;
                        case EXPECTED_INT:
                            ab.putInt((int)chnk.at8(rowIdx));
                            break;
                        case EXPECTED_LONG:
                            ab.put8(chnk.at8(rowIdx));
                            break;
                        case EXPECTED_DOUBLE:
                            ab.put8d(chnk.atd(rowIdx));
                            break;
                        case EXPECTED_STRING:
                            if (chnk.vec().isCategorical()) {
                                ab.putStr(chnk.vec().domain()[(int) chnk.at8(rowIdx)]);
                            } else if (chnk.vec().isString()) {
                                chnk.atStr(valStr, rowIdx);
                                ab.putStr(valStr.toString());
                            } else if (chnk.vec().isUUID()) {
                                UUID uuid = new UUID(chnk.at16h(rowIdx), chnk.at16l(rowIdx));
                                ab.putStr(uuid.toString());
                            } else {
                                assert false : "Can never be here";
                            }
                            break;
                    }

                }
                writeToChannel(ab, sock);
            }
        }
        ab.flipForReading().clearForWriting(H2O.MAX_PRIORITY);
        ab.put1(ExternalFrameHandler.CONFIRM_READING_DONE);
        writeToChannel(ab, sock);
    }



    private static void writeToChannel(AutoBuffer ab, SocketChannel channel) throws IOException {
        ab._bb.flip();
        while (ab._bb.hasRemaining()) {
            channel.write(ab._bb);
        }
    }
}
