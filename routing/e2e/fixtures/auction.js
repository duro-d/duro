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
