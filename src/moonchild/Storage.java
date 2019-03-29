package moonchild;

import java.io.*;
import java.util.HashMap;

public class Storage {
    HashMap<String, HashMap<Integer, Integer>> reference;

    //TableName,PageNum,Numberofthelastelement NOT index
    public Storage() {
        reference = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(DBApp.metadatastorage)));
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
                ret = Page.loadPage(tableName + page, page, arr, types);
                ret.number = page;
                break;
            }
        }
        if (ret == null) throw new DBAppException("Tuple Number Out of Bound");
        return ret;
    }

    int getPageNumbertofTupleNumber(String tableName, int idx) throws DBAppException {
        HashMap<Integer, Integer> hm = reference.get(tableName);
        for (int page = 0; page < hm.size(); page++) {
            if (idx < hm.get(page)) {
                return page;
            }
        }
        return 0;
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

    void UpdateTable(Table table) {
        reference.put(table.tablename, new HashMap<>());
        int cnt = 0;
        for (int i = 0; i < table.pages.size(); i++) {
            cnt += table.pages.get(i).tuples.size();
            reference.get(table.tablename).put(i, cnt);
        }

    }

}
