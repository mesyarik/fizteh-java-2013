package ru.fizteh.fivt.students.mescherinilya.multifilehashmap;

import java.io.File;
import java.io.IOException;

public class TableProviderFactory implements ru.fizteh.fivt.storage.structured.TableProviderFactory {

    @Override
    public TableProvider create(String dir) throws IllegalArgumentException, IOException {

        if (dir == null || dir.isEmpty() || dir.trim().isEmpty()) {
            throw new IllegalArgumentException("Name of the directory is empty!");
        }

        File newDir = new File(dir);


        if (!newDir.exists() && !newDir.mkdirs()) {
            throw new IllegalArgumentException("The directory doesn't exist and couldn't be created.");
        }

        if (!newDir.isDirectory() || !newDir.canRead() || !newDir.canWrite()) {
            throw new IllegalArgumentException("Bad root directory!");
        }

        TableProvider provider = new TableProvider(newDir);

        return provider;
    }


}
