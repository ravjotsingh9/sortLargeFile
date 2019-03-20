package sortservice.model;

import java.util.TreeSet;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Line {

    private String line;

    // To contain distinct word which appeared in line
    private TreeSet<String> set;

    public Line() {
    }

    public Line(String line, TreeSet<String> set) {
        this.line = line;
        this.set = new TreeSet<String>(set);
        
    }


    @Override
    public String toString() {
        return "line: " + line;
    }

}
