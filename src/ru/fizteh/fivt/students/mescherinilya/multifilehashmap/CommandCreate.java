package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import java.util.ArrayList;

public class CommandCreate implements Command {

    @Override
    public String getName() {
        return "create";
    }

    @Override
    public int getArgsCount() {
        return 2;
    }

    @Override
    public void execute(String[] args) throws Exception {

        String[] typenames = args[1].split("\\s+");

        ArrayList<Class<?>> columnTypes = new ArrayList<>();
        //System.out.println("+" + args[1] + "+");
        for (String typename : typenames) {
            typename = typename.trim();

            if (typename.equals("int"))
                columnTypes.add(Integer.class);
            else if (typename.equals("long"))
                columnTypes.add(Long.class);
            else if (typename.equals("byte"))
                columnTypes.add(Byte.class);
            else if (typename.equals("double"))
                columnTypes.add(Double.class);
            else if (typename.equals("float"))
                columnTypes.add(Float.class);
            else if (typename.equals("boolean"))
                columnTypes.add(Boolean.class);
            else if (typename.equals("String"))
                columnTypes.add(String.class);
            else {
                throw new Exception("Unsupported type in table: " + typename);
            }
        }

        ru.fizteh.fivt.storage.structured.Table newTable
                = MultiFileHashMap.provider.createTable(args[0], columnTypes);

        if (newTable == null) {
            System.out.println(args[0] + " exists");
        } else {
            System.out.println("created");
        }

    }
}
