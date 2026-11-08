function generate(ctx) {
    return {
        key: "batch-" + ctx.index,
        value: JSON.stringify({
            orderId: "ORD" + String(ctx.index).padStart(5, "0"),
            amount: Math.floor(ctx.random() * 1000) + 1,
            status: ctx.index % 10 === 0 ? "FAILED" : "CREATED",
            timestamp: ctx.time
        })
    };
}
