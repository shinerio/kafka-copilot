// Contract probe: enumerate every ctx member and the globals available in the
// goja sandbox. Used with --dry-run / dry_run:true; never sends to Kafka.
function generate(ctx) {
  return {
    key: "probe-" + ctx.index,
    value: JSON.stringify({
      keys: Object.keys(ctx).join(","),
      index: ctx.index,
      time: ctx.time,
      timeType: typeof ctx.time,
      r1: ctx.random(),
      r2: ctx.random(),
      uuid: ctx.uuid(),
      pickArr: ctx.pick(["A", "B", "C"]),
      pickObj: ctx.pick({ x: "X", y: "Y" }),
      pickEmpty: String(ctx.pick([])),
      padStart: "5".padStart(3, "0"),
      dateNow: new Date().getTime(),
      globals: {
        Math: typeof Math,
        JSON: typeof JSON,
        Date: typeof Date,
        Promise: typeof Promise,
        RegExp: typeof RegExp,
        Map: typeof Map,
        console: typeof console,
        require: typeof require,
        fetch: typeof fetch,
        setTimeout: typeof setTimeout,
        process: typeof process,
        module: typeof module,
        globalThis: typeof globalThis
      }
    })
  };
}
