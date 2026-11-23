export async function init(ctx, input) {
  if (!Number.isSafeInteger(input.limit) || input.limit < 0) ctx.reject("Invalid quota");
  await ctx.storage.put("remaining", input.limit);
}
export const methods = {
  async spend(ctx, { amount }) {
    const remaining = await ctx.storage.get("remaining");
    if (!Number.isSafeInteger(amount) || amount <= 0 || amount > remaining) ctx.reject("Insufficient quota");
    await ctx.storage.put("remaining", remaining - amount);
    return { remaining: remaining - amount };
  },
  async getRemaining(ctx) {
    return { remaining: await ctx.storage.get("remaining") };
  }
};
