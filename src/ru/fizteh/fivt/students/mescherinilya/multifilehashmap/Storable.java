package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;

import java.util.ArrayList;

public class Storable implements Storeable {

    private ArrayList<Object> contents;
    private ArrayList<Class> types;

    public Storable(ArrayList<Class> classes) {
        types = classes;
        contents = new ArrayList<>(types.size());
    }

    public Storable(ArrayList<Class> classes, ArrayList<Object> values) {
        if (classes.size() != values.size()) {
            System.err.println("Values list size doesn't match types list size!");
        }

        types = classes;
        contents = values;
    }

    @Override
    public void setColumnAt(int columnIndex, Object value) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!value.getClass().equals(types.get(columnIndex))) {
            throw new ColumnFormatException("Type mismatch at " + columnIndex + "th position!"
                    + "Expected " + types.get(columnIndex).getName()
                    + " but was given " + value.getClass().getName());
        }
        contents.set(columnIndex, value);
    }

    @Override
    public Object getColumnAt(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        return contents.get(columnIndex);
    }

    @Override
    public Integer getIntAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Integer.TYPE)) {
            throw new ColumnFormatException("You are requesting Integer, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Integer) contents.get(columnIndex);
    }

    @Override
    public Long getLongAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Long.TYPE)) {
            throw new ColumnFormatException("You are requesting Long, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Long) contents.get(columnIndex);
    }

    @Override
    public Byte getByteAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Byte.TYPE)) {
            throw new ColumnFormatException("You are requesting Byte, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Byte) contents.get(columnIndex);
    }

    @Override
    public Float getFloatAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Float.TYPE)) {
            throw new ColumnFormatException("You are requesting Float, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Float) contents.get(columnIndex);
    }

    @Override
    public Double getDoubleAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Double.TYPE)) {
            throw new ColumnFormatException("You are requesting Double, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Double) contents.get(columnIndex);
    }

    @Override
    public Boolean getBooleanAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(Boolean.TYPE)) {
            throw new ColumnFormatException("You are requesting Boolean, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (Boolean) contents.get(columnIndex);
    }

    @Override
    public String getStringAt(int columnIndex) throws ColumnFormatException, IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex >= contents.size()) {
            throw new IndexOutOfBoundsException();
        }
        if (!types.get(columnIndex).equals(String.class)) {
            throw new ColumnFormatException("You are requesting String, but there is "
                    + types.get(columnIndex).getName() + "!");
        }
        return (String) contents.get(columnIndex);
    }
}
