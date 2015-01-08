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
public class IdxData 
{
    private final long position;
    private final int numObjects;
    private String group;

    public IdxData(long position, int numObjects) {
        this.position = position;
        this.numObjects = numObjects;
    }

    public long getPosition() {
        return position;
    }

    public int getNumObjects() {
        return numObjects;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }

}