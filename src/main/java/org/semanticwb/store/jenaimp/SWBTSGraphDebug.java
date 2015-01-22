/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.store.jenaimp;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 *
 * @author javiersolis
 */
public class SWBTSGraphDebug extends GraphBase
{
    private Graph graph;
    
    public SWBTSGraphDebug(Graph graph)
    {
        this.graph=graph;
    }
    
    @Override
    protected ExtendedIterator<Triple> graphBaseFind(TripleMatch tm) 
    {
        return graph.find(tm);
    }

    @Override
    public void close() 
    {
        graph.close();
    }
    
    
    
}
