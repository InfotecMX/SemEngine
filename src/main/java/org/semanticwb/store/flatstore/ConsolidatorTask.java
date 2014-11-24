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
import java.util.Arrays;
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
            long sortTime = System.currentTimeMillis();
            List<FileTripleExtractor> archivos = IntStream.rangeClosed(1, numberChunks)
                    .mapToObj(index -> 
                    new FileTripleExtractor(GraphImp.getFilename(directory, name, index)))
                    .collect(toList());
            processJDK(archivos);
//            processInternal(archivos.toArray(new FileTripleExtractor[0]));
            System.out.println("Consolidation: "+(System.currentTimeMillis() - sortTime));
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
    
    private void processJDK(List<FileTripleExtractor> archivos){
        for(int i =0; i<triples; i++){
                archivos.sort(naturalOrder);
                FakeTriple ft = archivos.get(0).getCurrentTriple();
                save(ft);
                if (!archivos.get(0).consumeCurrentTriple()){
                    archivos.get(0).close();
                    archivos.get(0).delete();
                    archivos.remove(0);
                }
            }
    }
    
    private void processInternal(FileTripleExtractor[] archivos){
        FileTripleExtractor[] listaDatos = archivos;
        Arrays.sort(archivos, naturalOrder);
        for(int i =0; i<triples; i++){
                internalSorter(naturalOrder, listaDatos);
                FakeTriple ft = listaDatos[0].getCurrentTriple();
                save(ft);
                if (!listaDatos[0].consumeCurrentTriple()){
                    listaDatos[0].close();
                    listaDatos[0].delete();
                    listaDatos = Arrays.copyOfRange(listaDatos, 1, listaDatos.length);
                }
            }
    }
    
    private void internalSorter(Comparator naturalOrder, FileTripleExtractor[] archivos) {
        if (archivos.length < 1) return; //si sólo hay un elemento, ya está ordenado
        int currElePos = 0;
        int comparingTo = 1;
        while (currElePos < archivos.length - 1){
            if (naturalOrder.compare(archivos[currElePos], archivos[comparingTo])>0){
                FileTripleExtractor aux = archivos[currElePos];
                archivos[currElePos] = archivos[comparingTo];
                archivos[comparingTo] = aux;
                currElePos++;
                comparingTo++;
            } else break;
        }
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
                bof.write(bbf);
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
