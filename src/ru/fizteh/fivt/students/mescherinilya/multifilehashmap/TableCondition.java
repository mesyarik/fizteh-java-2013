package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.Storeable;

import java.util.Map;
import java.util.TreeMap;

// this class is for watching how many uncommitted changes are there in each table
public class TableCondition {

    Map<String, Storeable> added;
    Map<String, Storeable> deleted;

    TableCondition() {
        added = new TreeMap<>();
        deleted = new TreeMap<>();
    }

    void put(String key, Storeable value) {

        if (added.containsKey(key)) {
            added.remove(key);
        } else if (deleted.containsKey(key)) {
            deleted.remove(key);
        }

        Storeable oldValue = MultiFileHashMap.currentTable.get(key);

        if (oldValue != null && oldValue.equals(value)) {
            added.put(key, value);
        }
    }

    void remove(String key) {
        Storeable oldValue = MultiFileHashMap.currentTable.get(key);

        if (added.containsKey(key)) {
            added.remove(key);
            if (oldValue != null) {
                deleted.put(key, oldValue);
            }
        } else if (oldValue != null && !deleted.containsKey(key)) {
            deleted.put(key, oldValue);
        }
    }

    int changesCount() {
        return added.size() + deleted.size();
    }

    void clear() {
        if (added == null)
            System.out.println("fuck you!");
        added.clear();
        deleted.clear();
        System.out.println("We are in tablecondition.clear method!");
    }

}
