package org.semanticwb.store.flatstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import static java.util.stream.Collectors.toList;
import java.util.stream.IntStream;

/**
 *
 * @author serch
 */
public class ConsolidatorTask implements Runnable{
    private final int numberChunks;
    private final String name;
    private final File directory;
    private final long triples;
    private final RandomAccessFile dataFile;
    private final int BLOCK_SIZE=1_000;
    private final List<JumpFast> idxList = new ArrayList<>();
    
    private long tripleCounter = 0;
    

    public ConsolidatorTask(int numberChunks, String name, File directory, long triples) throws FileNotFoundException {
        this.numberChunks = numberChunks;
        this.name = name;
        this.directory = directory;
        this.triples = triples;
        this.dataFile = new RandomAccessFile(new File(directory, name+".swbdb"), "rw");
    }
    
    @Override
    public void run() {
        if (1<numberChunks){
            List<FileTripleExtractor> archivos = IntStream.rangeClosed(1, numberChunks)
                    .mapToObj(index -> 
                    new FileTripleExtractor(GraphImp.getFilename(directory, name, index)))
                    .collect(toList());
            for(int i =0; i<triples; i++){
                archivos.sort(naturalOrder);
                FakeTriple ft = archivos.get(0).getCurrentTriple();
                save(ft);
                if (!archivos.get(0).consumeCurrentTriple()){
                    archivos.get(0).close();
                    archivos.remove(0);
                }
            }
        }
        else {
            FileTripleExtractor fte = new FileTripleExtractor(GraphImp.getFilename(directory, name, 1));
            int count = 0;
            while (fte.getCurrentTriple()!= null){
                count++;
                if ((count % 1000)==0)
                    System.out.println("fte:"+fte.getCurrentTriple().getSubject());
                fte.consumeCurrentTriple();
            }
            fte.close();
        }
        saveIdx();
    }
    
    
    private final Function<FileTripleExtractor, String> bySubject = FileTripleExtractor::getCurrentSubject;
    private final Function<FileTripleExtractor, String> byProperty = FileTripleExtractor::getCurrentProperty;
    private final Function<FileTripleExtractor, String> byObject = FileTripleExtractor::getCurrentObject;
    
    
    public Comparator naturalOrder = Comparator.comparing(bySubject)
            .thenComparing(byProperty)
            .thenComparing(byObject);

    private long saveTriple(FakeTriple ft) {
        try {
            long position = dataFile.length();
            dataFile.seek(position);
            dataFile.write(ft.getDataBlock());
            return position;
        } catch (IOException ioe) {
            throw new RuntimeException("Writing to dataFile.", ioe);
        }
    }

    private void save(FakeTriple ft) {
        long triplePosition = (saveTriple(ft));
        if ((++tripleCounter) % BLOCK_SIZE == 0){
            idxList.add(new JumpFast(tripleCounter, triplePosition));
        }
        
    }

    private void saveIdx() {
        try {
            BufferedOutputStream bof = new BufferedOutputStream(
                    new FileOutputStream(new File(directory,name+".idx")));
            for (JumpFast jf : idxList){
                byte[] bbf = ByteBuffer.allocate(16)
                        .putLong(jf.getTripleNumber())
                        .putLong(jf.getPosition())
                        .array();
            }
            bof.close();
        } catch (IOException ioe) {
            throw new RuntimeException("Writing idx file", ioe);
        }
    }
    
    
}

class JumpFast {
    private final long tripleNumber;
    private final long position;

    public JumpFast(long tripleNumber, long position) {
        this.tripleNumber = tripleNumber;
        this.position = position;
    }

    public long getTripleNumber() {
        return tripleNumber;
    }

    public long getPosition() {
        return position;
    }
   
}
