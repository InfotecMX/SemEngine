/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.semanticwb.store.jenaimp;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.GraphView;

/**
 *
 * @author javiersolis
 */
public class SWBTSGraph2 extends GraphView
{
    // Switch this to DatasetGraphTransaction
    private final DatasetGraph    dataset ;

    public SWBTSGraph2(DatasetGraph dataset, Node graphName) {
        super(dataset, graphName) ;
        this.dataset = dataset ;
    }
    
}
