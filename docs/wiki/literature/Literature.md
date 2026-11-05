# Научная литература

Раздел собирает рецензируемые работы, на которые опирается проектная модель Duro. Главным источником проекта считаем [Durable Functions](Literature-Durable-Functions.md): эта работа отвечает на основной вопрос Duro – что именно платформа обязана гарантировать, чтобы прикладной код можно было писать, не обрабатывая отказы исполнителя. Она разобрана подробно, с формальной частью и выводами для архитектурного RFC; остальные работы разобраны короче и отвечают на частные вопросы.

Разборы передают содержание работ и нашу оценку их применимости. Они не задают контракт Duro: обязательное поведение первой версии определено в [требованиях к MVP](../Requirements.md), а проектная модель – в [концепциях](../concepts/Concepts.md).

Часть направлений лежит за границами MVP: локальные CRDT-реплики, смешанные уровни согласованности и синхронизация реплик. Эти работы считаем как подготовку к развитию, а не как требования PoC. Основания для отсрочки приведены в [анализе альтернатив](../concepts/Concepts-Alternatives.md#отложенные-возможности).

## Содержание

| Страница | Работа | Что даёт Duro |
| --- | --- | --- |
| [Durable Functions](Literature-Durable-Functions.md) – главный источник | Burckhardt и др., OOPSLA 2021 | Атомарная фиксация, повторы и границы гарантий; различие восстановления исполнения и данных |
| [Персистентные структуры данных](Literature-Persistent-Structures.md) | Driscoll и др., JCSS 1989 | Цена хранения версий; различие частичной, полной и сливающей персистентности |
| [TARDiS](Literature-TARDiS.md) | Crooks и др., SIGMOD 2016 | Ветка как понятие хранилища и минимальные сведения для слияния |
| [Eg-walker](Literature-Eg-Walker.md) | Gentle, Kleppmann, EuroSys 2025 | Граф событий как единственное долговечное представление реплики |
| [RedBlue consistency](Literature-RedBlue.md) | Li и др., OSDI 2012 | Критерий, какие операции можно выполнять локально |
| [SwiftCloud](Literature-SwiftCloud.md) | Zawirski и др., Middleware 2015 | Клиентская реплика, свежесть чтения и смена обслуживающего узла |
| [A fork() in the road](Literature-Fork.md) | Baumann и др., HotOS 2019 | Доводы против копирования процесса как механизма ветвления |
| [FoundationDB](Literature-FoundationDB.md) | Zhou и др., SIGMOD 2021 | Детерминированная симуляция как способ проверки отказов |

## Соответствие направлениям проекта

| Направление | Страницы Duro | Выбранная работа |
| --- | --- | --- |
| Модель объекта и восстановление исполнения | [Модель объекта](../concepts/Concepts-Object.md), [Исполнение](../concepts/Concepts-Execution.md) | Durable Functions |
| История данных и чтение прошлой ревизии | [Хранение и журнал](../concepts/Concepts-Persistence.md), [UC-04](../Use-Cases.md#uc-04-просмотреть-историю-и-прошлую-ревизию) | Персистентные структуры данных |
| Ветки и отложенное слияние | [История, ветки и откат](../concepts/Concepts-History.md), [UC-05](../Use-Cases.md#uc-05-создать-независимый-вариант) | TARDiS |
| Локальные реплики и офлайн-работа (вне MVP) | [Vision](../Vision.md) | Eg-walker |
| Операции, требующие серверной проверки (вне MVP) | [Vision](../Vision.md), [Альтернативы](../concepts/Concepts-Alternatives.md) | RedBlue consistency |
| Синхронизация реплик и смена узла (вне MVP) | [Vision](../Vision.md) | SwiftCloud |
| Граница снимка объекта | [Альтернативы](../concepts/Concepts-Alternatives.md#требования-к-снимкам-данных) | A fork() in the road |
| Проверка отказов и методика измерений | [NFR-02](../requirements/nonfunctional/NFR-02.md), [NFR-09](../requirements/nonfunctional/NFR-09.md) | FoundationDB |

## Порядок чтения

1. [Durable Functions](Literature-Durable-Functions.md) – главный источник: словарь для журнала, восстановления и повторов, устройство фиксации и формулировка гарантии.
2. [Персистентные структуры данных](Literature-Persistent-Structures.md) – цена версий и различие чтения прошлой версии от ветвления.
3. [TARDiS](Literature-TARDiS.md) – ветка как механизм хранилища.
4. [A fork() in the road](Literature-Fork.md) – почему снимок данных, а не снимок процесса.
5. [FoundationDB](Literature-FoundationDB.md) – как проверять поведение при отказах.
6. Далее направления за границами MVP: [Eg-walker](Literature-Eg-Walker.md), [RedBlue consistency](Literature-RedBlue.md), [SwiftCloud](Literature-SwiftCloud.md).

Первые пять работ относятся к обязательным операциям PoC. Последние три описывают возможности, отложенные в [альтернативах](../concepts/Concepts-Alternatives.md#отложенные-возможности).

## Пробелы в литературе

Каждое направление по отдельности в литературе покрыто. Хуже покрыты именно те стыки, на которых стоит модель Duro.

| Пробел | Почему он важен для Duro |
| --- | --- |
| Изоляция внешних действий в ветке | Есть семантика «не повторять внешний вызов при воспроизведении», но не «ветка по умолчанию не выполняет внешних действий». Вопрос открыт в [исполнении](../concepts/Concepts-Execution.md#внешние-действия) |
| Ветвление объекта с поведением | TARDiS и Decibel ветвят хранилище, Automerge – документ. Ветвление объекта с кодом, зафиксированной версией и собственной историей разобрано мало |
| Журнал ревизий вместе с репликами и серверными операциями | RedBlue и CISE выбирают уровень согласованности, но без версионной истории и веток |
| Сходимость после очень долгого отключения реплики | Проблема названа в local-first, затраты снижены в Eg-walker, но систематических измерений длительного офлайна мало |
| Воспроизведение истории при смене кода объекта | Обновление кода с миграциями не входит в MVP ([FR-10](../requirements/functional/FR-10.md)), а формальных решений для версионирования поведения известно немного |
| Сравнение снимков, журнала и копирования при записи | Для объектов с поведением готовых сравнительных бенчмарков нет; это естественная экспериментальная часть для [NFR-05](../requirements/nonfunctional/NFR-05.md) |

Это оценка по выбранной подборке, а не результат систематического обзора. Перед использованием в научном тексте её нужно подтвердить отдельным поиском.

## Литература

<a id="source-1"></a>

[1] Burckhardt S., Gillum C., Justo D., Kallas K., McMahon C., Meiklejohn C. S. [Durable Functions: Semantics for Stateful Serverless](https://doi.org/10.1145/3485510). Proceedings of the ACM on Programming Languages, т. 5, вып. OOPSLA, 2021. DOI: 10.1145/3485510.

<a id="source-2"></a>

[2] Driscoll J. R., Sarnak N., Sleator D. D., Tarjan R. E. [Making Data Structures Persistent](https://doi.org/10.1016/0022-0000(89)90034-2). Journal of Computer and System Sciences, 38(1): 86–124, 1989. DOI: 10.1016/0022-0000(89)90034-2.

<a id="source-3"></a>

[3] Crooks N., Pu Y., Estrada N., Gupta T., Alvisi L., Clement A. [TARDiS: A Branch-and-Merge Approach To Weak Consistency](https://doi.org/10.1145/2882903.2882951). SIGMOD 2016, с. 1615–1628. DOI: 10.1145/2882903.2882951.

<a id="source-4"></a>

[4] Gentle J., Kleppmann M. [Collaborative Text Editing with Eg-walker: Better, Faster, Smaller](https://doi.org/10.1145/3689031.3696076). EuroSys 2025, с. 311–328. DOI: 10.1145/3689031.3696076.

<a id="source-5"></a>

[5] Li C., Porto D., Clement A., Gehrke J., Preguiça N., Rodrigues R. [Making Geo-Replicated Systems Fast as Possible, Consistent when Necessary](https://www.usenix.org/conference/osdi12/technical-sessions/presentation/li). OSDI 2012, с. 265–278.

<a id="source-6"></a>

[6] Zawirski M., Preguiça N., Duarte S., Bieniusa A., Balegas V., Shapiro M. [Write Fast, Read in the Past: Causal Consistency for Client-Side Applications](https://doi.org/10.1145/2814576.2814733). Middleware 2015, с. 75–87. DOI: 10.1145/2814576.2814733.

<a id="source-7"></a>

[7] Baumann A., Appavoo J., Krieger O., Roscoe T. [A fork() in the road](https://doi.org/10.1145/3317550.3321435). HotOS 2019, с. 14–22. DOI: 10.1145/3317550.3321435.

<a id="source-8"></a>

[8] Zhou J., Xu M., Shraer A. и др. [FoundationDB: A Distributed Unbundled Transactional Key Value Store](https://doi.org/10.1145/3448016.3457559). SIGMOD 2021, с. 2653–2666. DOI: 10.1145/3448016.3457559.
