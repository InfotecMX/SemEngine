package org.semanticwb.store.flatstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import static java.util.Comparator.*;
import java.util.function.Function;
import org.semanticwb.store.TripleWrapper;

/**
 *
 * @author serch
 */
public class WriterTask implements Callable<String>{
    private final File filename;
    final private List<TripleWrapper> list;
    OutputStream out;
    
    
    private final Function<TripleWrapper, String> bySubject = TripleWrapper::getSubject;
    private final Function<TripleWrapper, String> byProp = TripleWrapper::getProperty;
    private final Function<TripleWrapper, String> byObj = TripleWrapper::getObject;
    
    public WriterTask(File filename, List<TripleWrapper> list) {
        this.filename = filename;
        this.list = list;
    }
    
    public void open() throws IOException {
        out = new BufferedOutputStream(new FileOutputStream(filename));
    }
    
    public void close() throws IOException {
        out.close();
    }
    
    @Override
    public String call() throws IOException {
        open();
        list.stream()
            .sorted(comparing(bySubject).thenComparing(byProp).thenComparing(byObj))
            .forEachOrdered(this::writeToFile);
        close();
        return filename.getName();
    }
    
    private void writeToFile(TripleWrapper triple) {
        try {
        byte[] sub = triple.getSubject().getBytes("utf-8");
        byte[] prop = triple.getProperty().getBytes("utf-8");
        byte[] obj = triple.getObject().getBytes("utf-8");
        int lt = sub.length + prop.length + obj.length + 20;
        byte[] data = ByteBuffer.allocate(lt).putInt(lt).putInt(sub.length)
                .putInt(prop.length).putInt(obj.length).put(sub).put(prop)
                .put(obj).putInt(lt).array();
        out.write(data);
        } catch (IOException ioe) {
            throw new WriterTaskException(ioe);
        }
    }
}

class WriterTaskException extends RuntimeException {
    public WriterTaskException(Exception e) {
        super("Can't convert to utf-8 bytes or can't write to file", e);
    }
}