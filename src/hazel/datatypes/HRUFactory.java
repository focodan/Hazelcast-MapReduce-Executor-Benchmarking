/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hazel.datatypes;

/**
 *
 * @author daniel.elliott
 */
public class HRUFactory {
    private static int id = 0;
    private static final int PAYLOAD_SIZE = 310;
    
    public static HRU getDefaultHRU(){
        HRU res = new HRU();
        
        res.ID = id;
        id++;
        
        res.payload = new double[PAYLOAD_SIZE];

        for(int i=0;i<PAYLOAD_SIZE;i++){
            (res.payload)[i] = i;
        }
        
        return res;
    }
    
    public static void resetIDcount(){
        id = 0;
    }
}
