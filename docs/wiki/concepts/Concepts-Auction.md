# Пример: аукцион

Аукцион показывает правила одного объекта: ставка должна расти, закрытый аукцион не принимает новые ставки, конкурентные вызовы проверяют подтверждённое состояние. Много аукционов создают нагрузку, которую можно распределять между исполнителями.

Это пример проектируемого API, а не уже доступный SDK. Аукцион закрывается явной командой; таймеры в MVP не нужны.

## JavaScript-пакет

```javascript
export async function init(ctx, input) {
  if (!Number.isSafeInteger(input.startPrice) || input.startPrice < 0) {
    ctx.reject("Некорректная начальная цена");
  }
  await ctx.storage.put("auction", {
    price: input.startPrice,
    bidder: null,
    closed: false
  });
}

export const methods = {
  async bid(ctx, input) {
    const auction = await ctx.storage.get("auction");
    if (auction.closed) ctx.reject("Аукцион закрыт");
    if (typeof input.bidder !== "string" || input.bidder.length === 0) {
      ctx.reject("Не указан участник");
    }
    if (!Number.isSafeInteger(input.amount) || input.amount <= auction.price) {
      ctx.reject("Ставка должна быть целым числом выше текущей цены");
    }
    const next = { ...auction, price: input.amount, bidder: input.bidder };
    await ctx.storage.put("auction", next);
    return next;
  },
  async close(ctx) {
    const auction = await ctx.storage.get("auction");
    const next = { ...auction, closed: true };
    await ctx.storage.put("auction", next);
    return next;
  },
  async getSummary(ctx) {
    return await ctx.storage.get("auction");
  }
};
```

Манифест указывает тип `Auction`, версию `1`, `runtime: javascript-v8`, `format: esm`, методы `bid`, `close`, `getSummary` и SHA-256 bundle. TypeScript можно использовать при разработке, но на сервер поступает собранный JavaScript.

## Вызовы клиента

```javascript
await duro.registerType(manifest, bundle);
await duro.create("auction:42", "Auction", "1", { startPrice: 100 }, {
  request_id: "create-auction-42"
});
await duro.invoke("auction:42", "bid", { bidder: "alice", amount: 120 }, {
  request_id: "bid-alice-1"
});
// → { result: { price: 120, bidder: "alice", closed: false } }
```

`put` внутри метода только готовит запись. Успех клиентского `invoke` означает, что состояние и результат уже долговечно зафиксированы. Исключение после `put` не оставляет новую ставку в БД.

Если ответ потерян, клиент повторяет вызов с `bid-alice-1` и теми же параметрами. Получает прежний ответ даже после восстановления на другой ноде. Новая ставка получает новый ID.

## Демонстрация highload

Генератор создаёт много аукционов, распределяет поток равномерно и с перекосом, меняет популярную группу. Сравниваются политики размещения, переносы и задержки. Остановка рабочей ноды и переключение БД не должны терять подтверждённые ставки.

Второй тип – квота с методами расхода и чтения остатка. Успешные списания не превышают начальный ресурс; повтор не списывает его дважды. Два типа подключаются одним контрактом без изменений ядра.

Предметные отказы аукциона и квоты учитываются отдельно от технических ошибок платформы. Для воспроизводимого замера runtime применяются также фиксированные обработчики из [NFR-05](../requirements/nonfunctional/NFR-05.md).
