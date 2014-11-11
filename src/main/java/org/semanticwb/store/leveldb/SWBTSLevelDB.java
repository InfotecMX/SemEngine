/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.semanticwb.store.leveldb;

import org.semanticwb.store.jenaimp.SWBTSModelMaker;
import org.semanticwb.store.jenaimp.SWBTStore;

/**
 *
 * @author javier.solis.g
 */
public class SWBTSLevelDB extends SWBTStore
{
    @Override
    public void init()
    {
        //log.event("SWBTSLevelDB initialized...");
        System.out.println("SWBTSLevelDB initialized...");
        setMaker(new SWBTSModelMaker("org.semanticwb.store.leveldb.GraphImp"));
    }
    
}
