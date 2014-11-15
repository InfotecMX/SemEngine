package org.semanticwb.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 *
 * @author serch
 */
public class FlatStoreFile {

    public static final String DATA_EXT = ".tsdb";

    private final String dataFilename;
    private RandomAccessFile db;
    private final Graph graph;

    public static FlatStoreFile create(String name, Graph graph)
            throws IOException {
        return new FlatStoreFile(name, true, graph);
    }

    public static FlatStoreFile open(String name, Graph graph)
            throws IOException {
        return new FlatStoreFile(name, false, graph);
    }

    private FlatStoreFile(String name, boolean clean, Graph graph)
            throws IOException {
        dataFilename = name + DATA_EXT;
        this.graph = graph;
        if (clean) {
            eraseFiles();
        }
        openFiles();
    }

    private void eraseFiles() {
        (new File(dataFilename)).delete();
    }

    private void openFiles() throws FileNotFoundException {
        db = new RandomAccessFile(new File(dataFilename), "rw");
    }

    public long add(Triple triple) throws IOException {
        byte[] sub = graph.encNode(triple.getSubject()).getBytes("utf-8");
        byte[] prop = graph.encNode(triple.getProperty()).getBytes("utf-8");
        byte[] obj = graph.encNode(triple.getObject()).getBytes("utf-8");
        long lt = sub.length + prop.length + obj.length + 28;
        byte[] startT = ByteBuffer.allocate(20).putLong(lt).putInt(sub.length)
                .putInt(prop.length).putInt(obj.length).array();
        byte[] endT = ByteBuffer.allocate(8).putLong(lt).array();
        long position = db.length();
        db.seek(position);
        db.write(startT, 0, startT.length);
        db.write(sub, 0, sub.length);
        db.write(prop, 0, prop.length);
        db.write(obj, 0, obj.length);
        db.write(endT, 0, endT.length);
        return position;
    }
    
    public void close() throws IOException {
        db.close();
    }

    public Triple readTriple(long position) throws IOException {
        db.seek(position);
        long lt = db.readLong();
        int subl = db.readInt();
        int prol = db.readInt();
        int objl = db.readInt();
        byte[] sub = new byte[subl]; 
        db.read(sub, 0 ,subl);
        byte[] pro = new byte[prol]; 
        db.read(pro, 0 ,prol);
        byte[] obj = new byte[objl]; 
        db.read(obj, 0 ,objl);
        return new Triple(graph.decNode(new String(sub)), 
                graph.decNode(new String(pro)), 
                graph.decNode(new String(obj)));
    }
    
    public long getNextTriplePosition(long position) throws IOException {
        db.seek(position);
        long next = db.readLong()+position;
        return (db.length()>next?next:-1);
    }
    
    public long getPreviousTriplePosition(long position) throws IOException {
        if (position-8 < 0) return -1;
        db.seek(position-8);
        return position - db.readLong();
    }
    
    public long getSize() throws IOException {
        return db.length();
    }
}
