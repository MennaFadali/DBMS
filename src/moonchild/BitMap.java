package moonchild;

import java.util.ArrayList;

public class BitMap {
    ArrayList<Character> bits;

    BitMap() {
        bits = new ArrayList<>();
    }

    BitMap(int n) {
        bits = new ArrayList<>();
        for (int i = 0; i < n; i++)
            bits.add('0');
    }

    void set(int idx) {
        bits.set(idx, '1');
    }

    void clear(int idx) {
        bits.set(idx, '1');
    }

    void toggle(int idx) {
        char neval = (bits.get(idx) == '0' ? '1' : '0');
        bits.set(idx, neval);
    }

    void addBitAfter(int idx, int val) {
//        String all = new String(bits);
//        bits = (all.substring(0, idx + 1) + '0' + all.substring(idx)).toCharArray();
        bits.add(idx + 1, val == 0 ? '0' : '1');
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (char x : bits)
            sb.append(x);
        return sb.toString();
    }


}
