# Обзор источников по Durable Objects

Для знакомства с моделью можно начать со статьи Kenton Varda 2020 года, затем прочитать о конкурентности и SQLite и перейти к примерам приложений. Гарантии API стоит сверять с документацией: авторские оценки удобства и архитектурных компромиссов не задают контракт платформы.

## Основные публикации

### Исходная модель

В статье описана начальная версия продукта до появления SQLite: идентичность объекта, прикладной код, приватное состояние и маршрутизация запросов [[1]](#source-1).

### Конкурентность и подтверждение записи

Статья разбирает гонки в однопоточном JavaScript, input gates, output gates и кеширование. На этих примерах видно, как правила runtime влияют на API хранения [[2]](#source-2).

### Отложенная работа

Авторы показывают, как разбудить объект без клиентского запроса и накопить работу перед обработкой [[3]](#source-3). Актуальные условия повторов описаны в Alarms API [[4]](#source-4).

### SQLite и Storage Relay Service

Статья описывает синхронный SQL, локальное исполнение запросов и устройство Storage Relay Service. Название относится к доступу к SQLite из прикладного кода; задержка клиентского запроса включает сеть и подтверждение записи. Условия beta из статьи устарели, текущий статус SQL нужно смотреть в документации [[5]](#source-5).

### Stateful serverless в PartyKit

Sunil Pai объясняет stateful serverless через адресуемый сервер для каждой сущности и описывает его применение в realtime-приложениях. Статья передаёт авторский взгляд на удобство модели и её компромиссы [[6]](#source-6).

### Архитектура AI чата

Pai предлагает объект на комнату и WebSocket для обмена сообщениями в AI чате. Восстановление незавершённого вызова внешней модели требует отдельного решения [[7]](#source-7).

### Хранение и синхронизация в tldraw

Команда описывает использование DO для комнат совместной доски и развитие её слоя сохранения на SQLite. Протокол синхронизации приложения реализован поверх инфраструктуры объекта [[8]](#source-8), [[9]](#source-9).

## Документация по темам

| Вопрос | Источник |
| --- | --- |
| Как определяет DO сама платформа | [[10]](#source-10) |
| Как выбирать границу и избегать типичных ошибок | [[11]](#source-11) |
| Когда экземпляр загружается и выгружается | [[12]](#source-12) |
| Что допустимо хранить в памяти | [[13]](#source-13) |
| Как найти объект и вызвать метод | [[14]](#source-14), [[15]](#source-15) |
| Какой порядок обеспечивает клиентская ссылка | [[16]](#source-16) |
| Как сохранять данные | [[17]](#source-17) |
| Как работают SQL, транзакции, KV и PITR | [[18]](#source-18) |
| Как блокировать конкурентную доставку событий | [[19]](#source-19) |
| Как сохранять WebSocket-соединения при выгрузке | [[20]](#source-20) |
| Что делать с ошибками и перегрузкой | [[21]](#source-21) |
| Какие есть тонкости уникальности и обновления кода | [[22]](#source-22) |
| Каковы лимиты и из чего складывается стоимость | [[23]](#source-23), [[24]](#source-24) |

## Связанные модели

- Microsoft Orleans использует модель виртуальных акторов с идентичностью, поведением и активацией по обращению [[25]](#source-25).
- Cloudflare Workflows сохраняет прогресс многошагового исполнения, поддерживает ожидания и повторы [[26]](#source-26).
- Transactional outbox согласует запись в БД с отправкой сообщения во внешнюю систему [[27]](#source-27).
- DO можно сочетать с SQL-БД, KV, object storage и очередями [[28]](#source-28).

Вернуться к [обзору Durable Objects](Durable-Objects.md).

## Литература

<a id="source-1"></a>

[1] Kenton Varda. [Workers Durable Objects Beta: A New Approach to Stateful Serverless](https://blog.cloudflare.com/introducing-workers-durable-objects/). Cloudflare Blog, 28 сентября 2020 года.

<a id="source-2"></a>

[2] Kenton Varda. [Durable Objects: Easy, Fast, Correct – Choose three](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/). Cloudflare Blog, 3 августа 2021 года.

<a id="source-3"></a>

[3] Matt Alonso, Greg McKeon. [Durable Objects Alarms – a wake-up call for your applications](https://blog.cloudflare.com/durable-objects-alarms/). Cloudflare Blog, 11 мая 2022 года.

<a id="source-4"></a>

[4] Cloudflare. [Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/). Документация.

<a id="source-5"></a>

[5] Kenton Varda. [Zero-latency SQLite storage in every Durable Object](https://blog.cloudflare.com/sqlite-in-durable-objects/). Cloudflare Blog, 26 сентября 2024 года.

<a id="source-6"></a>

[6] Sunil Pai. [The next evolution of serverless is stateful](https://sunilpai.dev/posts/the-future-of-serverless/). 6 февраля 2024 года.

<a id="source-7"></a>

[7] Sunil Pai. [Reliable UX for AI chat with Durable Objects](https://sunilpai.dev/posts/reliable-ux-for-ai-chat-with-durable-objects/). 27 января 2025 года.

<a id="source-8"></a>

[8] Steve Ruiz. [tldraw SDK 4.3](https://tldraw.dev/blog/tldraw-sdk-4.3). tldraw, 19 января 2026 года.

<a id="source-9"></a>

[9] tldraw. [tldraw sync](https://tldraw.dev/docs/sync). Документация.

<a id="source-10"></a>

[10] Cloudflare. [What are Durable Objects?](https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/). Документация.

<a id="source-11"></a>

[11] Cloudflare. [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/). Документация.

<a id="source-12"></a>

[12] Cloudflare. [Lifecycle of a Durable Object](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/). Документация.

<a id="source-13"></a>

[13] Cloudflare. [In-memory state in a Durable Object](https://developers.cloudflare.com/durable-objects/reference/in-memory-state/). Документация.

<a id="source-14"></a>

[14] Cloudflare. [Durable Object Namespace](https://developers.cloudflare.com/durable-objects/api/namespace/). Документация.

<a id="source-15"></a>

[15] Cloudflare. [Invoke methods](https://developers.cloudflare.com/durable-objects/best-practices/create-durable-object-stubs-and-send-requests/). Документация.

<a id="source-16"></a>

[16] Cloudflare. [Durable Object Stub](https://developers.cloudflare.com/durable-objects/api/stub/). Документация.

<a id="source-17"></a>

[17] Cloudflare. [Access Durable Objects Storage](https://developers.cloudflare.com/durable-objects/best-practices/access-durable-objects-storage/). Документация.

<a id="source-18"></a>

[18] Cloudflare. [SQLite-backed Durable Object Storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/). Документация.

<a id="source-19"></a>

[19] Cloudflare. [Durable Object State](https://developers.cloudflare.com/durable-objects/api/state/). Документация.

<a id="source-20"></a>

[20] Cloudflare. [Use WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/). Документация.

<a id="source-21"></a>

[21] Cloudflare. [Error handling](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/). Документация.

<a id="source-22"></a>

[22] Cloudflare. [Known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/). Документация.

<a id="source-23"></a>

[23] Cloudflare. [Limits](https://developers.cloudflare.com/durable-objects/platform/limits/). Документация.

<a id="source-24"></a>

[24] Cloudflare. [Pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/). Документация.

<a id="source-25"></a>

[25] Microsoft. [Microsoft Orleans](https://learn.microsoft.com/en-us/dotnet/orleans/overview). Microsoft Learn. Документация.

<a id="source-26"></a>

[26] Cloudflare. [Cloudflare Workflows](https://developers.cloudflare.com/workflows/). Документация.

<a id="source-27"></a>

[27] AWS. [Transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html). AWS Prescriptive Guidance.

<a id="source-28"></a>

[28] Cloudflare. [Choosing a data or storage product](https://developers.cloudflare.com/workers/platform/storage-options/). Документация.
