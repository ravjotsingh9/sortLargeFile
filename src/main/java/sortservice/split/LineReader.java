package sortservice.split;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;

import sortservice.config.Config;
import sortservice.model.Line;


public class LineReader implements ItemReader<Line>, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LineReader.class);

    private FileInputStream inputStream;
    private Scanner sc;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        try {
            this.inputStream = new FileInputStream(Config.SOURCE_FILE_PATH);
            this.sc = new Scanner(this.inputStream, "UTF-8");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        logger.debug("Line Reader initialized.");
    }

    @Override
    public synchronized Line read() throws Exception {
        String line = "";
        if(this.sc.hasNextLine()){
            line = sc.nextLine();
        } else {
            return null;
        }
        
        Line ln = new Line();
        ln.setLine(line);
        return ln;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        try {
            this.sc.close();
            this.inputStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.debug("Line Reader ended.");
        return ExitStatus.COMPLETED;
    }
}