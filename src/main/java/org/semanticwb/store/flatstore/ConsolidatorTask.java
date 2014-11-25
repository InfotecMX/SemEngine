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
    private int count = -1;
    private final int idx;
//    private final String sufix;
    private final Comparator cmp;
//    private final Comparator eval;

    private long tripleCounter = 0;

    public ConsolidatorTask(int numberChunks, String name, File directory,
            long triples, int idx) throws FileNotFoundException {
        this.numberChunks = numberChunks;
        this.idx = idx;
//        this.sufix = getSufix();
        this.cmp = getComparator();
//        this.eval = getEvaluator();
        this.name = name;
        this.directory = directory;
        this.triples = triples;
        this.dataFile = new RandomAccessFile(new File(directory, name + getSufix() + ".swbdb"), "rw");
    }

    @Override
    public void run() {
//        if (1 < numberChunks) {
        long sortTime = System.currentTimeMillis();
        List<FileTripleExtractor> archivos = IntStream.rangeClosed(1, numberChunks)
                .mapToObj(index
                        -> new FileTripleExtractor(getFilename(index)))
                .collect(toList());
//            processJDK(archivos);
        processInternal(archivos.toArray(new FileTripleExtractor[0]));
        System.out.println("Consolidation: " + (System.currentTimeMillis() - sortTime));
//        } else {
//            FileTripleExtractor fte = new FileTripleExtractor(GraphImp.getFilename(directory, name, 1));
//            int count = 0;
//            while (fte.getCurrentTriple() != null) {
//                count++;
//                if ((count % 1000) == 0) {
//                    System.out.println("fte:" + fte.getCurrentTriple().getSubject());
//                }
//                fte.consumeCurrentTriple();
//            }
//            fte.close();
//        }
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
        Arrays.sort(archivos, cmp);
        previous = getValue(archivos[0].getCurrentTriple());
        for (int i = 0; i < triples; i++) {
            internalSorter(cmp, listaDatos);
            FakeTriple ft = listaDatos[0].getCurrentTriple();
            save(ft);
            if (!listaDatos[0].consumeCurrentTriple()) {
                listaDatos[0].close();
                listaDatos[0].delete();
                listaDatos = Arrays.copyOfRange(listaDatos, 1, listaDatos.length);
            }
        }
    }
//
//    private Comparator getEvaluator() {
//        switch (idx) {
//            case 0:
//                return Comparator.comparing(FakeTriple::getSubject);
//            case 1:
//                return Comparator.comparing(FakeTriple::getProperty);
//            case 2:
//                return Comparator.comparing(FakeTriple::getObject);
//            default:
//                return null;
//        }
//    }

    private Comparator getComparator() {
        switch (idx) {
            case 0:
                return subjectOrder;
            case 1:
                return propertyOrder;
            case 2:
                return objectOrder;
            default:
                return null;
        }
    }

    private String getValue(FakeTriple fake) {
        switch (idx) {
            case 0:
                return fake.getSubject();
            case 1:
                return fake.getProperty();
            case 2:
                return fake.getObject();
            default:
                return null;
        }
    }

    private String getSufix() {
        switch (idx) {
            case 0:
                return "-sub";
            case 1:
                return "-pro";
            case 2:
                return "-obj";
            default:
                return null;
        }
    }

    private void internalSorter(Comparator naturalOrder, FileTripleExtractor[] archivos) {
        if (archivos.length < 1) {
            return; //si sólo hay un elemento, ya está ordenado
        }
        int currElePos = 0;
        int comparingTo = 1;
        while (currElePos < archivos.length - 1) {
            if (naturalOrder.compare(archivos[currElePos], archivos[comparingTo]) > 0) {
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

    private final Function<FileTripleExtractor, String> bySubject = FileTripleExtractor::getCurrentSubject;
    private final Function<FileTripleExtractor, String> byProperty = FileTripleExtractor::getCurrentProperty;
    private final Function<FileTripleExtractor, String> byObject = FileTripleExtractor::getCurrentObject;

    public Comparator subjectOrder = Comparator.comparing(bySubject)
            .thenComparing(byProperty)
            .thenComparing(byObject);
    public Comparator propertyOrder = Comparator.comparing(byProperty)
            .thenComparing(byObject)
            .thenComparing(bySubject);
    public Comparator objectOrder = Comparator.comparing(byObject)
            .thenComparing(bySubject)
            .thenComparing(byProperty);

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
//        try {
        long triplePosition = (saveTriple(ft));
        count++;
        idxList.add(new JumpFast(startPos, count));
//        if (null == getValue(ft) ){
//            System.out.println("FOUND Object with null --------> "+ idx + " ->" + getValue(ft));
//            System.out.println(" prev: "+ previous);
//            System.out.println(" ft-s: "+ft.getSubject());
//            System.out.println(" ft-p: "+ft.getProperty());
//            System.out.println(" ft-o: "+ft.getObject());
//            System.out.println("");
//        }
        if (!getValue(ft).equals(previous)) {
            previous = getValue(ft);
            count = 0;
            startPos = triplePosition;
        }
//        } catch (NullPointerException npe) {
//            System.out.println("NPE:");
//            System.out.println(" prev: "+ previous);
//            System.out.println(" ft-s: "+ft.getSubject());
//            System.out.println(" ft-p: "+ft.getProperty());
//            System.out.println(" ft-o: "+ft.getObject());
//            throw npe;
//        }
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
