/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.semanticwb.store.mem;

import org.semanticwb.store.jenaimp.SWBTSModelMaker;
import org.semanticwb.store.jenaimp.SWBTStore;

/**
 *
 * @author javier.solis.g
 */
public class SWBTSMem extends SWBTStore
{
    @Override
    public void init()
    {
        //log.event("SWBTSMem initialized...");
        System.out.println("SWBTSMem initialized...");
        setMaker(new SWBTSModelMaker("org.semanticwb.store.mem.GraphImp"));
    }
    
}
