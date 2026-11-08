package produce

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"
)

// scriptTimeout bounds a single script compilation / generate() call (FR-07).
const scriptTimeout = 5 * time.Second

// ProduceBatch executes script-based batch injection with backpressure pipeline (FR-07/08).
func (p *Producer) ProduceBatch(ctx context.Context, req *BatchRequest, progressFn func(produced, failed int64)) (*BatchResult, error) {
	start := time.Now()

	if req.Concurrency <= 0 {
		req.Concurrency = 8
	}
	if req.Concurrency > 32 {
		req.Concurrency = 32
	}
	if req.QueueCap <= 0 {
		req.QueueCap = 1024
	}

	// Resolve the script source. The Web UI sends inline JS source (a browser
	// file path is meaningless server-side); the CLI sends a local file path.
	scriptSrc := req.ScriptContent
	if scriptSrc == "" {
		if req.ScriptPath == "" {
			return nil, fmt.Errorf("script source is required (script_content or script path)")
		}
		data, err := os.ReadFile(req.ScriptPath)
		if err != nil {
			return nil, fmt.Errorf("read script: %w", err)
		}
		scriptSrc = string(data)
	}

	// Compile script in goja. Top-level code shares the same 5s budget as
	// generate() (FR-07 sandbox rule).
	vm := goja.New()
	compileTimer := time.AfterFunc(scriptTimeout, func() { vm.Interrupt("script compilation timeout") })
	_, err := vm.RunString(scriptSrc)
	compileTimer.Stop()
	vm.ClearInterrupt()
	if err != nil {
		return nil, fmt.Errorf("script compile error: %w", err)
	}

	generateVal, ok := goja.AssertFunction(vm.Get("generate"))
	if !ok {
		return nil, fmt.Errorf("script must export a generate(ctx) function")
	}

	// Dry-run: generate up to 5 samples, no sending.
	// A script may return a single message object or an array of them (FR-07),
	// so samples are counted per produced message, not per generate() call.
	if req.DryRun {
		samples := make([]Sample, 0, 5)
		count := req.Count
		if count <= 0 || count > 5 {
			count = 5
		}
		calls := 0
		built := 0
		for i := 0; i < count && len(samples) < 5; i++ {
			msgs, err := callGenerate(vm, generateVal, i)
			calls++
			if err != nil {
				return nil, fmt.Errorf("generate call %d: %w", i, err)
			}
			if msgs == nil { // null/undefined means "end of stream"
				break
			}
			if len(msgs) == 0 {
				return nil, fmt.Errorf("generate call %d returned an empty array; return one message object, a non-empty array, or null to end the stream", i)
			}
			built += len(msgs)
			for _, msg := range msgs {
				if len(samples) >= 5 {
					break
				}
				samples = append(samples, msgToSample(msg))
			}
		}
		return &BatchResult{
			DryRun:        true,
			Samples:       samples,
			GenerateCalls: int64(calls),
			MessagesBuilt: int64(built),
			ElapsedMs:     time.Since(start).Milliseconds(),
		}, nil
	}

	// Backpressure pipeline: bounded queue
	msgQueue := make(chan *genMessage, req.QueueCap)
	var produced, failed int64
	var cancelled atomic.Bool
	// Written only by the single generator goroutine, read after genWg.Wait().
	var genErr error
	var genCalls int64
	var genQueued int // messages actually handed to the send pipeline

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Generator goroutine
	var genWg sync.WaitGroup
	genWg.Add(1)
	go func() {
		defer genWg.Done()
		defer close(msgQueue)
		// --count is the total number of records; generate() is called until
		// that many messages have been queued, so array-returning scripts still
		// honour the requested count (FR-07). An empty array is rejected: it
		// would otherwise leave `genQueued` unchanged and loop forever.
		for i := 0; genQueued < req.Count; i++ {
			if cancelled.Load() || ctx.Err() != nil {
				return
			}
			atomic.AddInt64(&genCalls, 1)
			msgs, err := callGenerate(vm, generateVal, i)
			if err != nil {
				genErr = err
				return
			}
			if msgs == nil { // null/undefined ends the stream early
				return
			}
			if len(msgs) == 0 {
				genErr = fmt.Errorf("generate(%d) returned an empty array; return one message object, a non-empty array, or null to end the stream", i)
				return
			}
			for _, msg := range msgs {
				if genQueued >= req.Count { // last array is truncated to count
					return
				}
				select {
				case msgQueue <- msg:
					genQueued++
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Rate limiter
	var rateLimiter *time.Ticker
	if req.Rate > 0 {
		interval := time.Second / time.Duration(req.Rate)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}

	// Worker goroutines
	var sendWg sync.WaitGroup
	for w := 0; w < req.Concurrency; w++ {
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			for msg := range msgQueue {
				if cancelled.Load() || ctx.Err() != nil {
					return
				}
				if rateLimiter != nil {
					select {
					case <-rateLimiter.C:
					case <-ctx.Done():
						return
					}
				}
				rec := &kgo.Record{
					Topic:   req.Topic,
					Key:     msg.Key,
					Value:   msg.Value,
					Headers: msg.Headers,
					// Partition is derived from the key by the client.
				}
				err := p.client.ProduceSync(ctx, rec).FirstErr()
				if err != nil {
					atomic.AddInt64(&failed, 1)
				} else {
					p := atomic.AddInt64(&produced, 1)
					if p%500 == 0 && progressFn != nil {
						progressFn(p, atomic.LoadInt64(&failed))
					}
				}
			}
		}()
	}

	// Wait with graceful shutdown
	genWg.Wait()

	// Wait for senders with timeout (max 10s for in-flight)
	done := make(chan struct{})
	go func() {
		sendWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancelled.Store(true)
		cancel()
	}

	if ctx.Err() != nil {
		cancelled.Store(true)
	}
	if genErr != nil {
		// Already-confirmed records are not rolled back (FR-08), so include
		// the counts in the error instead of silently returning a short batch.
		return nil, fmt.Errorf("batch aborted after %d generate() calls, %d messages built (produced=%d failed=%d): %w",
			atomic.LoadInt64(&genCalls), genQueued,
			atomic.LoadInt64(&produced), atomic.LoadInt64(&failed), genErr)
	}

	elapsed := time.Since(start)
	result := &BatchResult{
		Produced:      atomic.LoadInt64(&produced),
		Failed:        atomic.LoadInt64(&failed),
		Cancelled:     cancelled.Load(),
		ElapsedMs:     elapsed.Milliseconds(),
		GenerateCalls: atomic.LoadInt64(&genCalls),
		MessagesBuilt: int64(genQueued),
	}
	if elapsed.Seconds() > 0 {
		result.TPS = float64(result.Produced) / elapsed.Seconds()
	}
	if progressFn != nil {
		progressFn(result.Produced, result.Failed)
	}
	return result, nil
}

type genMessage struct {
	Key     []byte
	Value   []byte
	Headers []kgo.RecordHeader
}

// callGenerate invokes the script's generate(ctx) once and returns every
// message it produced. A script may return a single message object, an array
// of message objects, or null/undefined to end the stream (nil, nil).
func callGenerate(vm *goja.Runtime, generate goja.Callable, index int) ([]*genMessage, error) {
	ctxObj := vm.NewObject()
	ctxObj.Set("index", index)
	ctxObj.Set("time", time.Now().UnixMilli())
	// math/rand/v2's top-level functions are concurrency-safe and seeded by
	// the runtime, so successive calls in the same millisecond still differ.
	ctxObj.Set("random", func() float64 {
		return rand.Float64()
	})
	ctxObj.Set("uuid", func() string {
		var b [16]byte
		for i := range b {
			b[i] = byte(rand.IntN(256))
		}
		b[6] = (b[6] & 0x0f) | 0x40 // version 4
		b[8] = (b[8] & 0x3f) | 0x80 // variant 10
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
	})
	ctxObj.Set("pick", func(call goja.FunctionCall) goja.Value {
		arr := call.Argument(0)
		if goja.IsNull(arr) || goja.IsUndefined(arr) {
			return goja.Undefined()
		}
		obj := arr.ToObject(vm)
		if obj == nil {
			return goja.Undefined()
		}
		keys := obj.Keys()
		if len(keys) == 0 {
			return goja.Undefined()
		}
		return obj.Get(keys[rand.IntN(len(keys))])
	})

	// Interrupt a runaway generator (e.g. `while(true){}`) after 5s. The
	// runtime is owned by the single generator goroutine, so this is safe.
	timer := time.AfterFunc(scriptTimeout, func() { vm.Interrupt("script execution timeout") })
	val, err := generate(goja.Undefined(), ctxObj)
	timer.Stop()
	vm.ClearInterrupt()
	if err != nil {
		return nil, fmt.Errorf("generate(%d): %w", index, err)
	}
	if val == nil || goja.IsUndefined(val) || goja.IsNull(val) {
		return nil, nil
	}

	if obj := val.ToObject(vm); obj != nil && obj.ClassName() == "Array" {
		n := int(obj.Get("length").ToInteger())
		out := make([]*genMessage, 0, n)
		for i := 0; i < n; i++ {
			item := obj.Get(fmt.Sprintf("%d", i))
			if item == nil || goja.IsUndefined(item) || goja.IsNull(item) {
				continue
			}
			msg, err := parseGenMessage(vm, item)
			if err != nil {
				return nil, err
			}
			if msg != nil {
				out = append(out, msg)
			}
		}
		return out, nil
	}

	msg, err := parseGenMessage(vm, val)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return []*genMessage{msg}, nil
}

func parseGenMessage(vm *goja.Runtime, val goja.Value) (*genMessage, error) {
	obj := val.ToObject(vm)
	if obj == nil {
		return &genMessage{
			Key:   autoKey(),
			Value: []byte(val.String()),
		}, nil
	}

	msg := &genMessage{}

	if keyVal := obj.Get("key"); keyVal != nil && !goja.IsUndefined(keyVal) && !goja.IsNull(keyVal) {
		msg.Key = []byte(keyVal.String())
	}
	if valVal := obj.Get("value"); valVal != nil && !goja.IsUndefined(valVal) && !goja.IsNull(valVal) {
		msg.Value = []byte(valVal.String())
	}
	// Scripts cannot pin a partition: the client derives it from the key.
	if hdrVal := obj.Get("headers"); hdrVal != nil && !goja.IsUndefined(hdrVal) && !goja.IsNull(hdrVal) {
		hdrs := hdrVal.ToObject(vm)
		if hdrs != nil {
			for _, key := range hdrs.Keys() {
				v := hdrs.Get(key)
				msg.Headers = append(msg.Headers, kgo.RecordHeader{
					Key:   key,
					Value: []byte(v.String()),
				})
			}
		}
	}

	if len(msg.Value) == 0 {
		return nil, fmt.Errorf("generate must return an object with a 'value' field")
	}
	if len(msg.Key) == 0 {
		msg.Key = autoKey()
	}

	return msg, nil
}

// autoKey guarantees every produced record has a key, so partitioning stays
// deterministic by key even when a generator omits the field (FR-06/07).
var autoKeySeq uint64

func autoKey() []byte {
	return []byte(fmt.Sprintf("kp-auto-%d-%d", time.Now().UnixMilli(), atomic.AddUint64(&autoKeySeq, 1)))
}

func msgToSample(msg *genMessage) Sample {
	s := Sample{
		Value: string(msg.Value),
	}
	if len(msg.Key) > 0 {
		s.Key = string(msg.Key)
	}
	if len(msg.Headers) > 0 {
		s.Headers = make(map[string]string)
		for _, h := range msg.Headers {
			s.Headers[h.Key] = string(h.Value)
		}
	}
	return s
}
