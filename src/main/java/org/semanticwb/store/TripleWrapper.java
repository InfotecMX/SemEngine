/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.semanticwb.store;

import java.nio.ByteBuffer;

/**
 *
 * @author serch
 */
public class TripleWrapper {
    final private Triple triple;
    final private Graph graph;
    private String subject = null;
    private String property = null;
    private String object = null;
    
    public TripleWrapper(Triple triple, Graph graph) {
        this.triple = triple;
        this.graph = graph;
    }
    
    public String getSubject() {
        if (null == subject) subject = graph.encNode(triple.getSubject());
        return subject;
    }
    
    public String getProperty() {
        if (null == property) property = graph.encNode(triple.getProperty());
        return property;
    }
    
    public String getObject() {
        if (null == object) object = graph.encNode(triple.getObject());
        return object;
    }
    
    public static Triple getTripleFromData(Graph graph, byte[] data){
        ByteBuffer bb = ByteBuffer.wrap(data);
        int iSub = bb.getInt(4);
        int iProp = bb.getInt(8);
        int iObj = bb.getInt(12);
        return new Triple(
                graph.decNode(new String(data, 16, iSub)), 
                graph.decNode(new String(data, 16 + iSub, iProp)), 
                graph.decNode(new String(data, 16 + iSub + iProp, iObj)));
    }
    
}
