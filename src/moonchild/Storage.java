package moonchild;

import java.io.*;
import java.util.HashMap;

public class Storage {
    HashMap<String, HashMap<Integer, Integer>> reference;

    //TableName,PageNum,Numberofthelastelement
    public Storage() {
        reference = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("/data/metadatastorage.csv")));
            while (br.ready()) {
                String Line[] = br.readLine().split(",");
                if (!reference.containsKey(Line[0])) reference.put(Line[0], new HashMap<>());
                reference.get(Line[0]).put(Integer.parseInt(Line[1]), Integer.parseInt(Line[2]));
            }
        } catch (FileNotFoundException e) {
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    Page getPagetofTupleNumber(String tableName, int idx) throws DBAppException {
        Page ret = null;
        HashMap<Integer, Integer> hm = reference.get(tableName);
        for (int page = 0; page < hm.size(); page++) {
            if (idx < hm.get(page)) {
                String[] arr = Table.getArrangements(tableName);
                HashMap<String, String> types = Table.getArrangementType(tableName);
                ret = Page.loadPage(tableName, arr, types);
                ret.number = page;
                break;
            }
        }
        if (ret == null) throw new DBAppException("Tuple Number Out of Bound");
        return ret;
    }

    void SaveStorage() {
        try {
            FileWriter fw = new FileWriter(new File(DBApp.metadatastorage));
            for (String tablename : reference.keySet())
                for (int page : reference.get(tablename).keySet())
                    fw.write(tablename + "," + page + "," + reference.get(tablename).get(page) + "\n");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
