
package hazel.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * @author daniel.elliott
 */
public class ResultWriter {
    
    private String header;
    private List<Long[]> data;
    
    public ResultWriter(){
        header = null;
        data = null;
    }
    
    public void setHeader(String header){
        this.header = header;
    }
    
    public void setData(List<Long[]> data){
        this.data = data;
    }

    // Method based on code from webpage
    // http://stackoverflow.com/questions/10667734/java-file-open-a-file-and-write-to-it
    // authored May 19 2012 by "Hovercraft Full Of Eels"
    public void write(String fileName) {
        PrintWriter pw = null;

        try {
            File file = new File(fileName);
            FileWriter fw = new FileWriter(file, true);
            pw = new PrintWriter(fw);
            
            if(header != null){
                pw.println(header);
            }
            if(data != null){
                for(Long[] row : data){
                        for(int i=0;i<row.length;i++){
                            pw.print(row[i]);
                            if(i!=row.length-1) pw.print(",");
                        }pw.println();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }
}
