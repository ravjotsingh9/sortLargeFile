package sortservice.split;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;

import sortservice.config.Config;
import sortservice.model.Line;


public class LinesWriter  implements ItemWriter<Line>, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesWriter.class);

    private BufferedWriter writer;
    
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("Line Writer initialized.");
    }

    @Override
    public synchronized void write(List<? extends Line> lines) throws Exception {
        try {
            // Create a new writer for the intermediate output file
            writer = new BufferedWriter(new FileWriter(Config.DESTINATION_LOCATION+"data_"+ (System.currentTimeMillis())+ ".part"));
            
            // Create another set to merge the sets from all lines read in a chunk
            TreeSet<String> mainSet = new TreeSet<String>();

            // Traverse over all lines in chunk and merge their sets to mainset for this chunk
            for (Line line : lines) {
                TreeSet<String> set = line.getSet();
                Iterator<String> itr = set.iterator();
                while (itr.hasNext()) {
                    mainSet.add(itr.next());
                }
            }

            // Iterate over the mainset and dump to the writer
            Iterator<String> itr = mainSet.iterator();
            while (itr.hasNext()) {
                this.writer.write(itr.next()+ "\n");  
            }

            // Close the writer
            this.writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }      
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("Line Writer ended.");
        return ExitStatus.COMPLETED;
    }
}