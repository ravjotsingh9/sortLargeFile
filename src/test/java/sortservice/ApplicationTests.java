package sortservice;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.StringBufferInputStream;
import java.io.BufferedReader;
import java.io.FileReader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

	@Test
	public void check_AllWordsAreUnique() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("output/out.full"));
		HashSet<String> sortedWordsSet = new HashSet<String>();

		String line;
        while ((line = br.readLine()) != null) {
			if(sortedWordsSet.contains(line)){
				assertTrue(false);
			}
			sortedWordsSet.add(line);
		}
	}

	@Test
	public void check_AllWordsAreSorted() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("output/out.full"));
		HashSet<String> sortedWordsSet = new HashSet<String>();

        String line;
        String prev = br.readLine();
        
        while ((line = br.readLine()) != null) {
			
			if(line.compareTo(prev) <= 0){
				assertTrue(false);
			}
			prev = line;
		}
	}

	
}
