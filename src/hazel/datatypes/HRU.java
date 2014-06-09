/** 
 * This HRU is a simplification of the Ages-W implementation.
 * In particular, I made three simplifications.
 * 1. The special arrays (such as HRU[] and StreamReach[]) are not considered.
 * 2. The OMS annotations are removed.
 * 3. The 177 doubles and 44 double arrays have been condensed into a single double array of 310 elements.
 * 
 * This will be serialized at a similar size to a 'true' HRU type.
 */

package hazel.datatypes;

import java.io.Serializable;

public class HRU implements Serializable {

    public int ID;

    public double[] payload;

    @Override
    public String toString() {
        return "HRU[id=" + ID + "]";
    }
}
