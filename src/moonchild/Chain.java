package moonchild;

import java.util.HashMap;

public class Chain {
    Node head;

    public Chain() {
        this.head = new Node();
    }

    public void addFirst(Object val) {
        Node n = new Node(val);
        n.next = head;
        head.prev = n;
        head = n;
    }

    public void resolveAnd() {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("AND")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(BitMap.and((BitMap) prev.value, (BitMap) next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }

    public void resolveXOR() {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("XOR")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(BitMap.xor((BitMap) prev.value, (BitMap) next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }

    public void resolveOr() {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("OR")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(BitMap.or((BitMap) prev.value, (BitMap) next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }

    public void resolveAnd(HashMap<String, Object> hm) {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("AND")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(DBApp.satisfies(hm, prev.value) && DBApp.satisfies(hm, next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }

    public void resolveXOR(HashMap<String, Object> hm) {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("XOR")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(DBApp.satisfies(hm, prev.value) ^ DBApp.satisfies(hm, next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }

    public void resolveOr(HashMap<String, Object> hm) {
        Node cur = head;
        while (cur != null) {
            if (cur.value instanceof String && cur.value.equals("OR")) {
                Node prev = cur.prev;
                Node next = cur.next;
                Node result = new Node(DBApp.satisfies(hm, prev.value) || DBApp.satisfies(hm, next.value));
                result.next = next.next;
                result.prev = prev.prev;
                if (prev.prev == null) head = result;
                else prev.prev.next = result;
                if (next.next != null) next.next.prev = result;
                next.prev = null;
                next.next = null;
                prev.prev = null;
                prev.next = null;
                cur = result.next;
            } else cur = cur.next;
        }
    }


    @Override
    public String toString() {
        String ans = "";
        Node cur = head;
        while (cur != null) {
            ans += cur + "->";
            cur = cur.next;
        }
        return ans;
    }

    static class Node {
        Node next, prev;
        Object value;

        Node() {

        }

        Node(Object val) {
            this.value = val;
        }

        @Override
        public String toString() {
            return value == null ? "null" : value.toString();
        }
    }
}
