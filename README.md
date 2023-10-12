Скрипт *handler.py* написан без использования сторонних библиотек, таких как **Pandas** и **Pydantic**, поэтому должен запускаться при наличии только лишь Python3.

Скрипт используется для извлечения данных из csv файлов, валидации и
сохранения их в файл как спецификацию json-rpc для дальнейшей отправки на API.

Справка по работе скрипта:

```commandline
python3 -m handler -h
```

Получить json файл для данных price. Запуск скрипта здесь происходит из рабочей директории, в этой же директории расположен файл price.csv.

```commandline
python3 -m handler price price.csv
```

Получить json файл для данных inventory

```commandline
python3 -m handler inventory inventory2.csv
```
Если в файле csv отсутствуют столбцы обрабатываемой модели, нужно воспользоваться флагом --ignore
```commandline
python3 -m handler inventory inventory1.csv --ignore stock_in_days,in_transit
```

Запись логов ведется в stdout, логи можно настроить в скрипте.


Логи пишутся, если в обязательном поле ошибка или значение отсутствует, так же если указано ошибочное значение в необязательном поле.

Заметила, что в файле inventory1.csv отсутствует два последних необязательных столбца, сделала довольно топорный способ обработки. Но все же это не давало покоя, и решила, что нужно обработать исключение любых необязательных столбцов, не важно в каком порядке. Пришлось переписать почти весь скрипт, но я осталась довольна добавлением флага --ignore.

Если бы данные обработчики писались внутри приложения, я бы обязательно воспользовалась библиотеками Pandas и Pydantic для более быстрой и точной работы с csv и валидацией данных.

Сделала упор на валидацию данных, а не их корректировку. Но для catigories в price и time в inventory все же сделала коррекцию. 
