package org.semanticwb.store.flatstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import org.semanticwb.store.Graph;
import org.semanticwb.store.Triple;
import org.semanticwb.store.TripleWrapper;

/**
 *
 * @author serch
 */
public class TripleFileReader {

    private final RandomAccessFile data;
    private final RandomAccessFile idx;
    private final String baseFilename;
    private final Graph graphReference;

    private long idxPosition = 0;
    //private long dataPosition = 0;

    public TripleFileReader(String baseFilename, Graph graphReference) throws FileNotFoundException {
        this.baseFilename = baseFilename;
        this.graphReference = graphReference;
        data = new RandomAccessFile(this.baseFilename + ".swbdb", "r");
        idx = new RandomAccessFile(this.baseFilename + ".idx", "r");
    }

    public void reset() {
        idxPosition = 0;
    }
/*
    public Triple[] getTripleGroup() throws IOException {
        return getTripleGroup(getIdxData());
    }
*/
    public void advance() {
        idxPosition += 12;
    }

    public long count() throws IOException {
        long ret = 0;
        reset();
        long size = idx.length() / 12;
        for (int i = 0; i < size; i++) {
            ret += getIdxData().getNumObjects();
            advance();
        }
        return ret;
    }

    private IdxData getIdxData() throws IOException {
        idx.seek(idxPosition);
        byte[] idxData = new byte[12];
        idx.read(idxData);
        ByteBuffer bb = ByteBuffer.wrap(idxData);
        return new IdxData(bb.getLong(), bb.getInt());
    }

    private IdxData getNextIdxData() throws IOException {
        idxPosition += 12;
        return getIdxData();
    }
/*
    private Triple[] getTripleGroup(IdxData idx) throws IOException {
        Triple[] ret = new Triple[idx.getNumObjects()];
        long currPosition = idx.getPosition();
        for (int i = 0; i < idx.getNumObjects(); i++) {
            byte[] buff = getDataBlock(currPosition);
            ret[i] = TripleWrapper.getTripleFromData(graphReference, buff);
            currPosition += buff.length;
        }
        return ret;
    }

    private Triple getTripleAt(long position) throws IOException {
        return TripleWrapper.getTripleFromData(graphReference,
                getDataBlock(position));
    }
*/
    private byte[] getDataBlock(long position) throws IOException {
        data.seek(position);
        byte[] header = new byte[4];
        data.read(header);
        int size = ByteBuffer.wrap(header).getInt();
        byte[] buff = new byte[size];
        data.seek(position);
        data.read(buff);
        return buff;
    }

    private String getSubjectAt(long position) throws IOException {
        byte[] buff = getDataBlock(position);
        int subSize = ByteBuffer.wrap(buff).getInt(4);
        return new String(buff, 16, subSize, "utf-8");
    }

    void close() throws IOException {
        if (null != data) {
            data.close();
        }
        if (null != idx) {
            idx.close();
        }
    }
}

class IdxData {

    private final long position;
    private final int numObjects;

    public IdxData(long position, int numObjects) {
        this.position = position;
        this.numObjects = numObjects;
    }

    public long getPosition() {
        return position;
    }

    public int getNumObjects() {
        return numObjects;
    }

}
