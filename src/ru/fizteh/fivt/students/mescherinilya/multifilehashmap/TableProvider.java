package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TableProvider implements ru.fizteh.fivt.storage.structured.TableProvider {

    File rootDir;
    HashMap<String, Table> tables;

    TableProvider(File dir) {
        rootDir = dir;

        if (rootDir == null) {
            throw new IllegalArgumentException("Root dir is null!");
        }

        if (!rootDir.isDirectory()) {
            throw new IllegalArgumentException("Bad root directory!");
        }

        tables = new HashMap<>();

        for (File cub : rootDir.listFiles()) {
            if (cub.isDirectory()) {
                List<Class<?>> valueTypes = new ArrayList<>();
                try (RandomAccessFile signFile =
                             new RandomAccessFile(cub.getAbsolutePath() + File.separator + "signature.tsv", "r")) {
                    String[] signature = signFile.readLine().split("\\s+");
                    for (String typename : signature) {
                    //here some govnocode
                        if (typename.equals("int")) {
                            valueTypes.add(Integer.class);
                        } else if (typename.equals("long")) {
                            valueTypes.add(Long.class);
                        } else if (typename.equals("byte")) {
                            valueTypes.add(Byte.class);
                        } else if (typename.equals("boolean")) {
                            valueTypes.add(Boolean.class);
                        } else if (typename.equals("float")) {
                            valueTypes.add(Float.class);
                        } else if (typename.equals("double")) {
                            valueTypes.add(Double.class);
                        } else if (typename.equals("String")) {
                            valueTypes.add(String.class);
                        } else {
                            throw new IOException("Unknown data type: " + typename + "!");
                        }
                    }
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    continue;
                }

                Table table = new ru.fizteh.fivt.students.mescherinilya.multifilehashmap.Table
                        (cub.getName(), valueTypes, this);
                tables.put(cub.getName(), table);
            }
        }

    }

    boolean isBadName(String name) {
        if (name == null || name.isEmpty() || name.trim().isEmpty()) {
            return true;
        }

        for (int i = 0; i < name.length(); ++i) {
            char c = name.charAt(i);
            if (c == '\\' || c == '/' || c == '.' || c == ':' || c == '*'
                    || c == '?' || c == '|' || c == '"' || c == '<' || c == '>'
                    || c == ' ') {
                return true;
            }
        }

        return false;
    }

    @Override
    public Table getTable(String name) throws IllegalArgumentException {
        if (isBadName(name)) {
            throw new IllegalArgumentException("Bad table name!");
        }

        if (tables.containsKey(name)) {
            return tables.get(name);
        } else {
            return null;
        }

    }

    @Override
    public Table createTable(String name, List<Class<?>> columnTypes) throws IOException {
        if (isBadName(name)) {
            throw new IllegalArgumentException("Bad table name!");
        }

        if (columnTypes == null) {
            throw new IllegalArgumentException("List of column types is null!");
        }

        if (tables.containsKey(name)) {
            return null;
        } else {
            File path = new File(rootDir.getAbsolutePath() + File.separator + name);
            if (!path.mkdir()) {
                System.err.println("Couldn't create a new directory.");
            }
            File signFile = new File(path.getAbsoluteFile() + File.separator + "signature.tsv");
            if (!signFile.createNewFile()) {
                throw new IOException("Couldn't create signature file!");
            }
            try (RandomAccessFile sgnFile = new RandomAccessFile(signFile.getAbsolutePath(), "rw")) {
                String signature = "";
                for (Class<?> columnType : columnTypes) {
                // and now some more govnocode
                    if (columnType == Integer.class) {
                        signature = signature + "int ";
                    } else if (columnType == Long.class) {
                        signature = signature + "long ";
                    } else if (columnType == Byte.class) {
                        signature = signature + "byte ";
                    } else if (columnType == Boolean.class) {
                        signature = signature + "boolean ";
                    } else if (columnType == Float.class) {
                        signature = signature + "float ";
                    } else if (columnType == Double.class) {
                        signature = signature + "double ";
                    } else if (columnType == String.class) {
                        signature = signature + "String ";
                    } else {
                        throw new IllegalArgumentException(
                                "Something very strange happened while creating table...");
                    }
                }
                sgnFile.writeBytes(signature);
            }

            Table newTable =
                    new ru.fizteh.fivt.students.mescherinilya.multifilehashmap.Table(name, columnTypes, this);

            tables.put(name, newTable);
            return newTable;
        }


    }

    @Override
    public void removeTable(String name) throws IllegalArgumentException, IllegalStateException {
        if (isBadName(name)) {
            throw new IllegalArgumentException("Bad table name!");
        }

        if (tables.containsKey(name)) {
            File victimTable = new File(rootDir.getAbsoluteFile() + File.separator + name);

            for (Integer i = 0; i < 16; ++i) {
                String dirName = i.toString() + ".dir";

                for (Integer j = 0; j < 16; ++j) {
                    String fileName = dirName + File.separator + j.toString() + ".dat";

                    File victim = new File(victimTable.getAbsoluteFile() + File.separator + fileName);
                    if (victim.exists() && !victim.delete()) {
                        System.err.println("Couldn't delete the file " + fileName);
                    }

                }

                File victimDir = new File(victimTable.getAbsoluteFile() + File.separator + dirName);
                if (victimDir.exists() && !victimDir.delete()) {
                    System.err.println("Couldn't delete the directory " + dirName
                            + ". Maybe there are some unexpected files inside it.");
                }

            }

            if (!victimTable.delete()) {
                throw new IllegalArgumentException("Couldn't delete the directory " + name);
                //System.err.println("Couldn't delete the directory " + name);
            }

            tables.remove(name);

        } else {
            throw new IllegalStateException("The table doesn't exist!");
        }


    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        Storeable result = createFor(table);

        if (!value.startsWith("<row>")) {
            throw new ParseException("Opening tag <row> is missing!", 0);
        }
        value = value.substring(5);

        for (int i = 0; i < table.getColumnsCount(); ++i) {
            if (value.startsWith("<null/>")) {
                value = value.substring(7);
                result.setColumnAt(i, null);
            } else if (value.startsWith("<col>")) {
                value = value.substring(5);
                if (value.indexOf("</col>") == -1) {
                    throw new ParseException("There is no tag </col> for the " + i + "th column!", 0);
                }
                String core = value.substring(0, value.indexOf("</col>"));
                Class<?> columnType = table.getColumnType(i);
                //System.out.print(columnType.getName());
                Object parsed = null;
                try {
                    if (columnType == Integer.class) {
                        parsed = Integer.parseInt(core);
                    } else if (columnType == Long.class) {
                        parsed = Long.parseLong(core);
                    } else if (columnType == Byte.class) {
                        parsed = Byte.parseByte(core);
                    } else if (columnType == Boolean.class) {
                        parsed = Boolean.parseBoolean(core);
                    } else if (columnType == Double.class) {
                        parsed = Double.parseDouble(core);
                    } else if (columnType == Float.class) {
                        parsed = Float.parseFloat(core);
                    } else {
                        parsed = core;
                    }
                } catch (NumberFormatException e) {
                    throw new ParseException("Couldn't parse " + columnType.getName()
                            + " from the string \"" + core + "\"!", 0);
                }
                //System.out.print(parsed.getClass().getName());
                result.setColumnAt(i, parsed);
                value = value.substring(core.length() + 6);
                //System.out.print("ffgdff");

            } else if (value.length() == 0) {
                throw new ParseException("Unexpected end of string!", 0);
            } else {
                throw new ParseException("Expected \'<\' but found \'" + value.charAt(0)
                        + "\' before " + i + "th column!", 0);
            }
        }

        if (!value.equals("</row>")) {
            throw new ParseException("Missing tag </row> in the end of string!", 0);
        }

        return result;
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        int columnsCount = table.getColumnsCount();
        String result = "<row>";
        for (int i = 0; i < columnsCount; ++i) {
            Object columnValue = value.getColumnAt(i);
            if (columnValue != null && !columnValue.getClass().equals(table.getColumnType(i))) {
                throw new ColumnFormatException("Type mismatch at the " + i + "th position!");
            }
            if (columnValue == null) {
                result = result + "<null/>";
            } else {
                result = result + "<col>" + columnValue.toString() + "</col>";
            }
        }
        result = result + "</row>";
        return result;
    }

    @Override
    public Storeable createFor(Table table) {
        ArrayList<Class> classes = new ArrayList<>();
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            classes.add(table.getColumnType(i));
        }
        //System.out.print(classes.size());
        return new Storable(classes);
    }

    @Override
    public Storeable createFor(Table table, List<?> values) throws ColumnFormatException, IndexOutOfBoundsException {
        if (values.size() != table.getColumnsCount()) {
            throw new IndexOutOfBoundsException("Inappropriate size of the values list!");
        }
        ArrayList<Class> classes = new ArrayList<>();
        ArrayList<Object> contents = new ArrayList<>();
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            if (!table.getColumnType(i).equals(table.getColumnType(i))) {
                throw new ColumnFormatException("Type mismatch at " + i + "th position!"
                        + "Expected " + table.getColumnType(i).getName()
                        + " but was given " + table.getColumnType(i).getName());
            }
            classes.add(table.getColumnType(i));
            contents.add(values.get(i));
        }
        return new Storable(classes, contents);
    }


}
