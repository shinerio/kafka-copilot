// Generator that deliberately omits the `key` field.
// The tool must synthesize a unique `kp-auto-*` key for every record so that
// partitioning still stays deterministic by key (FR-06 / FR-07).
function generate(ctx) {
    return {
        value: JSON.stringify({
            orderId: "NOKEY" + String(ctx.index).padStart(5, "0"),
            amount: Math.floor(ctx.random() * 1000) + 1,
            status: ctx.index % 2 === 0 ? "FAILED" : "CREATED",
            timestamp: ctx.time
        })
    };
}
