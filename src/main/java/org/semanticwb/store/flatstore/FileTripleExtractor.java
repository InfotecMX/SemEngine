package org.semanticwb.store.flatstore;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 * @author serch
 */
public class FileTripleExtractor {

    private final InputStream inFile;
    private final String filename;
    private Iterator<FakeTriple> localIt;
    private long position = 0;
    private FakeTriple currentTriple = null;

    public FileTripleExtractor(String filename) {
        this.filename = filename;
        try {
            inFile = new BufferedInputStream(new FileInputStream(this.filename));
            if (!inFile.markSupported()) {
                throw new IOException("mark is not supported...");
            }
            currentTriple = getNextTripleOnFile();
        } catch (IOException ioe) {
            throw new RuntimeException("Opening File for reading:" + this.filename, ioe);
        }
    }

    public FakeTriple getCurrentTriple() {
        return currentTriple;
    }

    public boolean consumeCurrentTriple() {
        try {
            currentTriple = getNextTripleOnFile();
            return null != currentTriple;
        } catch (IOException ioe) {
            throw new RuntimeException("Getting a Triple", ioe);
        }
    }

    private FakeTriple getNextTripleOnFile() throws IOException {
        inFile.mark(4);
        byte[] ltb = new byte[4];
        int rb = inFile.read(ltb);
        if (-1 == rb) {
            return null;
        }
        int lt = ByteBuffer.wrap(ltb).getInt();
        inFile.reset();
        byte[] data = new byte[lt];
        rb = inFile.read(data);
        if (-1 == rb) {
            return null;
        }
        return new FakeTriple(data);
    }

    public void close() {
        try {
            inFile.close();
        } catch (IOException ioe) {
            throw new RuntimeException("Clossing the file", ioe);
        }
    }

    public String getCurrentIdxData(){
        return currentTriple.getData();
    }
    
    public boolean delete() {
        return new File(filename).delete();
    }
    
}
