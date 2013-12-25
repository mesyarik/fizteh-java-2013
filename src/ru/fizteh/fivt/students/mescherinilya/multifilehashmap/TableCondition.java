package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.Storeable;

import java.util.Map;

// this class is for watching how many uncommitted changes are there in each table
public class TableCondition {

    Map<String, Storeable> added;
    Map<String, Storeable> deleted;

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
        added.clear();
        deleted.clear();
    }

}
