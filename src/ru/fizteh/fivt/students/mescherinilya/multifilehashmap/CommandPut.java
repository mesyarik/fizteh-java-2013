package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.Storeable;

import java.text.ParseException;

public class CommandPut implements Command {

    @Override
    public int getArgsCount() {
        return 2;
    }

    @Override
    public void execute(String[] args) throws ParseException {
        if (MultiFileHashMap.currentTable == null) {
            System.out.println("no table");
            return;
        }

        Storeable deserialized = MultiFileHashMap.provider.deserialize(
                MultiFileHashMap.currentTable, args[1]);

        Storeable oldValue = MultiFileHashMap.currentTable.put(args[0], deserialized);
        MultiFileHashMap.currentTableCondition.put(args[0], deserialized);

        if (oldValue != null) {
            System.out.println("overwrite\n" + oldValue);
        } else {
            System.out.println("new");
        }

    }

    @Override
    public String getName() {
        return "put";
    }
}
