package org.semanticwb.store.flatstore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
public class ConsolidatorTask implements Runnable {

    private final int numberChunks;
    private final String name;
    private final File directory;
    private final long triples;
    private final RandomAccessFile dataFile;
    private final List<JumpFast> idxList = new ArrayList<>();
    private String previous = null;
    private long startPos = 0;
    private int count = 0;
    private final IdxBy idx;
    private final Function<FileTripleExtractor, String> byData = FileTripleExtractor::getCurrentIdxData;
    private final Comparator cmp = Comparator.comparing(byData);      

    private long tripleCounter = 0;

    public ConsolidatorTask(int numberChunks, String name, File directory,
            long triples, IdxBy idx) throws FileNotFoundException {
        this.numberChunks = numberChunks;
        this.idx = idx;
        this.name = name;
        this.directory = directory;
        this.triples = triples;
        this.dataFile = new RandomAccessFile(new File(directory, name + getSufix() + ".swbdb"), "rw");
    }

    @Override
    public void run() {
        long sortTime = System.currentTimeMillis();
        List<FileTripleExtractor> archivos = IntStream.rangeClosed(1, numberChunks)
                .mapToObj(index
                        -> new FileTripleExtractor(getFilename(index)))
                .collect(toList());
//            processJDK(archivos);
        processInternal(archivos.toArray(new FileTripleExtractor[0]));
        System.out.println("Consolidation: " + (System.currentTimeMillis() - sortTime));
        saveIdx();
    }

    private String getFilename(int index) {
        try {
            return GraphImp.getFilename(directory, name, index).getCanonicalPath() + getSufix();
        } catch (IOException ioe) {
            throw new RuntimeException("Converting filename ", ioe);
        }
    }

    /*
     private void processJDK(List<FileTripleExtractor> archivos) {
     Comparator cmp = getComparator();
     for (int i = 0; i < triples; i++) {
     archivos.sort(cmp);
     FakeTriple ft = archivos.get(0).getCurrentTriple();
     save(ft);
     if (!archivos.get(0).consumeCurrentTriple()) {
     archivos.get(0).close();
     archivos.get(0).delete();
     archivos.remove(0);
     }
     }
     }
     */
    private void processInternal(FileTripleExtractor[] archivos) {
        FileTripleExtractor[] listaDatos = archivos;
        Arrays.sort(listaDatos, cmp);
        previous = listaDatos[0].getCurrentTriple().getGroup();
        for (int i = 0; i < triples; i++) {
            internalSorter(cmp, listaDatos);
            FakeTriple ft = listaDatos[0].getCurrentTriple();
//            System.out.println("ft:"+ft.getSubject()+" "+ft.getProperty()+" "+ft.getObject());
            save(ft);
            if (!listaDatos[0].consumeCurrentTriple()) {
                listaDatos[0].close();
                listaDatos[0].delete();
                listaDatos = Arrays.copyOfRange(listaDatos, 1, listaDatos.length);
                
                System.out.println("listaDatos:"+listaDatos.length+" "+triples+" "+i+" "+(triples-i));
            }
        }
        idxList.add(new JumpFast(startPos, count));
    }

    private String getSufix() {
        switch (idx) {
            case SUBJECT:
                return "-sub";
            case PROPERTY:
                return "-pro";
            case OBJECT:
                return "-obj";
            default:
                return null;
        }
    }

    private void internalSorter(Comparator comparator, FileTripleExtractor[] archivos) {
        if (archivos.length < 1) {
            return; //si sólo hay un elemento, ya está ordenado
        }
        int currElePos = 0;
        int comparingTo = 1;
        while (currElePos < archivos.length - 1) {
            if (comparator.compare(archivos[currElePos], archivos[comparingTo]) > 0) {
                FileTripleExtractor aux = archivos[currElePos];
                archivos[currElePos] = archivos[comparingTo];
                archivos[comparingTo] = aux;
                currElePos++;
                comparingTo++;
            } else {
                break;
            }
        }
    }

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

    FakeTriple last=null;
    private void save(FakeTriple ft) {
        if(!ft.equals(last))
        {
            last=ft;
            long triplePosition = saveTriple(ft);
            if (!ft.getGroup().equals(previous)) {
                    idxList.add(new JumpFast(startPos, count));
                    previous = ft.getGroup();
                    count = 0;
                    startPos = triplePosition;
            }
            count++;
        }
    }

    private void saveIdx() {
        try {
            BufferedOutputStream bof = new BufferedOutputStream(
                    new FileOutputStream(new File(directory, name + getSufix() + ".idx")));
            for (JumpFast jf : idxList) {
                byte[] bbf = ByteBuffer.allocate(12)
                        .putLong(jf.getPosition())
                        .putInt(jf.getnumSubjects())
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

    private final long position;
    private final int numSubjects;

    public JumpFast(long position, int numSubjects) {
        this.numSubjects = numSubjects;
        this.position = position;
    }

    public int getnumSubjects() {
        return numSubjects;
    }

    public long getPosition() {
        return position;
    }

}
