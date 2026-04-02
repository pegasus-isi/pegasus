/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.classes;

public class Tuple<X, Y> {
    public final X x;
    public final Y y;

    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X getKey() {
        return this.x;
    }

    public Y getValue() {
        return this.y;
    }

    public String toString() {
        return this.getKey() + " , " + this.getValue();
    }
}
