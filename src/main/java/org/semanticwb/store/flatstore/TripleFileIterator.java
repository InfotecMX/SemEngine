/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.store.flatstore;

/**
 *
 * @author javiersolis
 */
public class TripleFileIterator {
    private TripleFileReader reader;
    
    private long idxPosition = 0;
    private long dataPosition = 0;    

    public TripleFileIterator(TripleFileReader reader) {
        this.reader=reader;
    }
    
    
}
