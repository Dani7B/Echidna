package org.piggyback;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class FromTupleToBag extends EvalFunc<DataBag> {
	
     TupleFactory tupleFactory = TupleFactory.getInstance();
     BagFactory bagFactory = BagFactory.getInstance();
 
     public DataBag exec(Tuple input) throws IOException {
         
             DataBag output = bagFactory.newDefaultBag();
             Object o = input.get(0);
             
             if (!(o instanceof Tuple)) {
            	 throw new IOException("Expected input to be of type Tuple, but got " + o.getClass().getName());
             }
             
             Tuple t = (Tuple) o;
             for(int i=0; i<t.size(); i++){
            	 output.add(tupleFactory.newTuple(t.get(i)));
             }
             return output;
     }
 }