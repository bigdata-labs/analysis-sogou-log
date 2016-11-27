package codes.showme;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by jack on 11/13/16.
 */
public class SogouLogAnalysisTest {

    @Test
    public void testName() throws Exception {
        String line = "21:52:53\t8416132741258622\t[马英九+简历]\t1 1\tnews.xinhuanet.com/tw/2008-03/22/content_7839364.htm\n";

        for (String s : Arrays.asList(line.split("\\s"))) {
            System.out.println(s);
        }

    }
}