package moonchild;

import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;


/**
 * The TableFile is store in the dataFolder by its name it contains two  things :
 * 1.FirstLine : Number of pages in the Table
 * 3.SecondLine : The arrangement of the columns in the Tuple
 **/


public class Table implements Serializable {
    Vector<Page> pages;
    String tablename;
    String[] arr;

    public Table(String tablename) {
        this.tablename = tablename;
        pages = new Vector<>();
    }

    static Table loadTable(String name) throws DBAppException {
        Table ans = new Table(name);
        HashMap<String, String> types = getArrangementType(name);
        try {
            String path = "data/" + name;
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);
            int n = Integer.parseInt(br.readLine());
            String[] ar = br.readLine().split(",");
            ans.arr = ar;
            for (int i = 0; i < n; i++)
                ans.pages.add(Page.loadPage(name + i, ar, types));
        } catch (FileNotFoundException e) {
            throw new DBAppException("Sorry, there is no table with this name");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans;
    }


    static void saveTable(Table table) {
        String path = "data/" + table.tablename;
        int n = table.pages.size();
        saveArrangements(table.tablename, table.arr, n);
        for (int i = 0; i < n; i++)
            Page.savePage(table.pages.get(i), table.arr);
    }

    static String[] getArrangements(String name) {
        String[] ans = null;
        try {
            FileReader fr = new FileReader("data/" + name);
            BufferedReader br = new BufferedReader(fr);
            br.readLine();
            ans = br.readLine().split(",");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans;
    }

    static void saveArrangements(String name, String[] arr, int pageNum) {
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File("data/" + name));
            fw.write(pageNum + "\n");
            for (String x : arr)
                fw.write(x + ",");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static HashMap<String, String> getArrangementType(String tablename) {
        HashMap<String, String> ans = new HashMap<String, String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
            while (br.ready()) {
                String[] line = br.readLine().split(",");
                if (line[0].equals(tablename)) ans.put(line[1], line[2]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans;
    }

    static void saveArrangements(String name, Hashtable<String, String> ht, int pageNum) {
        String[] arr = new String[ht.size()];
        int i = 0;
        for (String x : ht.keySet())
            arr[i++] = x;
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File("data/" + name));
            fw.write(pageNum + "\n");
            for (String x : arr)
                fw.write(x + ",");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}