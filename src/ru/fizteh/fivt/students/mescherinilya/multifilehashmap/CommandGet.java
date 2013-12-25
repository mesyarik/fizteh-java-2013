package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.Storeable;

public class CommandGet implements Command {

    public CommandGet() {
        super();
    }

    @Override
    public String getName() {
        return "get";
    }
    @Override
    public int getArgsCount() {
        return 1;
    }

    @Override
    public void execute(String[] args) {

        if (MultiFileHashMap.currentTable == null) {
            System.out.println("no table");
            return;
        }
        Storeable value = MultiFileHashMap.currentTable.get(args[0]);
        if (value == null) {
            System.out.println("not found");
        } else {
            System.out.println("found\n" + value);
        }

    }

}
