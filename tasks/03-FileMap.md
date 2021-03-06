## FileMap

Консольное приложение, которое работает с файлом-БД. Файл называется ```db.dat```. Файл должен располагаться
в директории, которая получается из property ```System.getProperty("fizteh.db.dir")```. Чтобы задать директории
из командной строки, надо запускать приложение так: ```java -Dfizteh.db.dir=/home/student ru.fizteh.fivt.students.test.DbMain```.

Изменения не записываются на диск до выполнения команды commit.
В случае, если пользователь ошибся во вводе, должна быть возможность сделать rollback - откат изменений к последнему commit'у.

Приложение должно работать как в интерактивном режиме, так и в пакетном (см. задание Shell).

Приложение должно поддерживать следующие команды с фиксированным выводом:

### put
```
put key value
```

Если значение новое, то вывод:
```
new
```

Если значение затирает существующее:
```
overwrite
old value
```

### get
```
get key
```

Если значение есть:
```
found
value
```

Если значения нет:
```
not found
```

### remove
```
remove key
```

Если значение удалено:
```
removed
```

Если значения нет:
```
not found
```

### size
```
size
```

Вывести число хранимых пар ключ-значение:
```
6
```

### commit
```
commit
```

Записать изменения на диск.
Вывод - число измененных значений:
```
int
```

### rollback
```
rollback
```

Откатить все изменения до предыдущего коммита (или до состояния на момент начала программы).
Вывод - число откатанных изменений:
```
int
```

### quit
```
quit
```

Выход из системы - завершение работы программы. В случае, если есть несохраненные изменения, не выходить, вывести число несохраненных изменений:
```
5 unsaved changes
```

### Вариант 1
Данные лежат в бинарном файле следующего формата:
```
Длина ключа 1, длина значения 1, ключ 1, значение 1,
Длина ключа 2, длина значения 2, ключ 2, значение 2,
...
```

Форматы длин - целые числа в машинном представлении, 4 байта.
Формат ключа и значения - байты, полученные из строки в кодировке UTF-8.

### Вариант 2
Данные лежат в бинарном файле следующего формата:
```
Ключ 1, \0, смещение значения 1,
Ключ 2, \0, смещение значения 2,
...
Ключ N, \0, смещение значения N,
значение 1, значение 2, ...
```

Форматы смещений - целые числа в машинном представлении, 4 байта.
Формат ключа - байты, полученные из строки в кодировке UTF-8. Оканчивается ключ нулевым символом (\0).
Между списком смещений и первым значением нет разрыва.
