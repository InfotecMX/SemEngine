/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mem.test;

import java.io.File;
import org.semanticwb.store.flatstore.FakeTriple;
import org.semanticwb.store.flatstore.FileTripleExtractor;

/**
 *
 * @author serch
 */
public class TripleComparator {

    /**
     * @param args the command line arguments
     */
    public static void main2(String[] args) {
        FileTripleExtractor ftp1 = new FileTripleExtractor(new File("/Users/serch/Proyects/gitRepos/SemEngine/demo/HomePages.swbdb"));
        FileTripleExtractor ftp2 = new FileTripleExtractor(new File("/Users/serch/Proyects/gitRepos/SemEngine/demo1/HomePages.swbdb"));
        long count = 0;
        while (ftp1.getCurrentTriple() != null) {
            count++;
            if (!getString(ftp1.getCurrentTriple()).equals(getString(ftp2.getCurrentTriple()))) {
                System.out.println("Triple: " + count);
                System.out.println("1: " + ftp1.getCurrentSubject() + " | " + ftp1.getCurrentProperty() + " | " + ftp1.getCurrentObject());
                System.out.println("1: " + ftp2.getCurrentSubject() + " | " + ftp2.getCurrentProperty() + " | " + ftp2.getCurrentObject());
            }
            ftp1.consumeCurrentTriple();
            ftp2.consumeCurrentTriple();
        }
        ftp1.close();
        ftp2.close();
    }

    public static void main(String[] args) {
        FileTripleExtractor ftp1 = new FileTripleExtractor(new File("/Users/serch/Proyects/gitRepos/SemEngine/demo/HomePages.swbdb"));
        long count = 0;
        for (int x = 0; x < 200; x++) {
            count++;
            System.out.println("Triple: " + count);
            System.out.println("1: " + ftp1.getCurrentSubject() + " | " + ftp1.getCurrentProperty() + " | " + ftp1.getCurrentObject());
            ftp1.consumeCurrentTriple();
        }
        ftp1.close();
    }

    static String getString(FakeTriple triple) {
        return triple.getSubject() + "|" + triple.getProperty() + "|" + triple.getObject();
    }
}
