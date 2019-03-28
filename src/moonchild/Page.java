package moonchild;

import java.io.*;
import java.util.HashMap;
import java.util.Vector;

public class Page implements Serializable {
    //The Page consists of a vector of tuples where each tuple is a hashmap that maps the col name to its value
    Vector<HashMap<String, Object>> tuples;
    String name;
    int number;

    Page(String name , int n) {
        tuples = new Vector<>();
        this.name = name;
        number = n;
    }

    public Page() {
        tuples = new Vector<>();
    }


    static Page loadPage(String name, int number, String[] arrangement, HashMap<String, String> types) throws DBAppException {
        Page ans = new Page(name , number);
        ans.number = number;
        try {
            FileReader fr = new FileReader(new File("data/" + name));
            BufferedReader br = new BufferedReader(fr);
            while (br.ready()) {
                String[] Line = br.readLine().split(",");
                HashMap<String, Object> row = new HashMap<>();
                for (int i = 0; i < Line.length; i++)
                    row.put(arrangement[i], DBApp.convert(Line[i], types.get(arrangement[i])));
                ans.tuples.add(row);
            }
        } catch (FileNotFoundException e) {
            throw new DBAppException("Sorry, there is no Page with this name");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans;
    }

    static void savePage(Page page, String[] arrangement) {
        String path = "data/" + page.name;
        try {
            FileWriter fw = new FileWriter(new File(path));
            for (HashMap<String, Object> hm : page.tuples) {
                for (int i = 0; i < arrangement.length; i++)
                    fw.write(hm.get(arrangement[i]) + ",");
                fw.write("\n");
            }
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    //For Testing purposes only
    void printPage() {
        for (HashMap<String, Object> hm : tuples) {
            for (String col : hm.keySet())
                System.out.print(col + " " + hm.get(col) + ",");
            System.out.println();
        }
    }

}