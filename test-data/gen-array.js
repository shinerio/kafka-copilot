// Verifies FR-07: generate(ctx) may return an ARRAY of messages, some without
// a key (auto-generated), and ctx.random()/pick()/uuid() must vary per call.
function generate(ctx) {
    return [
        {
            key: "arr-" + ctx.index + "-a",
            value: JSON.stringify({
                orderId: "ARR" + String(ctx.index).padStart(5, "0"),
                amount: Math.floor(ctx.random() * 1000) + 1,
                status: ctx.pick(["CREATED", "PAID", "FAILED"]),
                trace: ctx.uuid()
            })
        },
        {
            value: JSON.stringify({
                orderId: "ARR" + String(ctx.index).padStart(5, "0") + "-B",
                amount: Math.floor(ctx.random() * 1000) + 1,
                status: ctx.pick(["CREATED", "PAID", "FAILED"]),
                trace: ctx.uuid()
            })
        }
    ];
}
