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

    static BitMap and(BitMap one, BitMap two) {
        BitMap ans = new BitMap();
        for (int i = 0; i < one.bits.size(); i++)
            ans.bits.add(BitOperations(one.bits.get(i), two.bits.get(i), 0));
        return ans;
    }

    static BitMap or(BitMap one, BitMap two) {
        BitMap ans = new BitMap();
        for (int i = 0; i < one.bits.size(); i++)
            ans.bits.add(BitOperations(one.bits.get(i), two.bits.get(i), 1));
        return ans;
    }

    static BitMap xor(BitMap one, BitMap two) {
        BitMap ans = new BitMap();
        for (int i = 0; i < one.bits.size(); i++)
            ans.bits.add(BitOperations(one.bits.get(i), two.bits.get(i), 2));
        return ans;
    }

    static BitMap not(BitMap one) {
        BitMap ans = new BitMap();
        for (int i = 0; i < one.bits.size(); i++)
            ans.bits.add(not(one.bits.get(i)));
        return ans;
    }

    static char not(char a) {
        return a == '1' ? '0' : '1';
    }

    static char BitOperations(char a, char b, int type) {
        //add => 0
        //or => 1
        //xor => 2
        switch (type) {
            case 0:
                if (a == '0' || b == '0') return '0';
                else return '1';
            case 1:
                if (a == '1' || b == '1') return '1';
                else return '0';
            case 2:
                if (a == b) return '0';
                else
                    return '1';
        }
        return '0';
    }

    int get(int idx) {
        return (bits.get(idx) == '0') ? 0 : 1;
    }

    void set(int idx) {
        bits.set(idx, '1');
    }

    void clear(int idx) {
        bits.set(idx, '1');
    }

    void toggle(int idx) {
        bits.set(idx, not(bits.get(idx)));
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
