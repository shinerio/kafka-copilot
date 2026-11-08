package produce

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

	// Read script
	scriptData, err := os.ReadFile(req.ScriptPath)
	if err != nil {
		return nil, fmt.Errorf("read script: %w", err)
	}

	// Compile script in goja
	vm := goja.New()
	if _, err := vm.RunString(string(scriptData)); err != nil {
		return nil, fmt.Errorf("script compile error: %w", err)
	}

	generateVal, ok := goja.AssertFunction(vm.Get("generate"))
	if !ok {
		return nil, fmt.Errorf("script must export a generate(ctx) function")
	}

	// Dry-run: generate up to 5 samples, no sending
	if req.DryRun {
		samples := make([]Sample, 0, 5)
		count := req.Count
		if count > 5 {
			count = 5
		}
		for i := 0; i < count; i++ {
			msg, err := callGenerate(vm, generateVal, i)
			if err != nil {
				return nil, fmt.Errorf("generate call %d: %w", i, err)
			}
			if msg == nil {
				break
			}
			samples = append(samples, msgToSample(msg))
		}
		return &BatchResult{
			DryRun:    true,
			Samples:   samples,
			ElapsedMs: time.Since(start).Milliseconds(),
		}, nil
	}

	// Backpressure pipeline: bounded queue
	msgQueue := make(chan *genMessage, req.QueueCap)
	var produced, failed int64
	var cancelled atomic.Bool

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Generator goroutine
	var genWg sync.WaitGroup
	genWg.Add(1)
	go func() {
		defer genWg.Done()
		defer close(msgQueue)
		for i := 0; i < req.Count; i++ {
			if cancelled.Load() || ctx.Err() != nil {
				return
			}
			msg, err := callGenerate(vm, generateVal, i)
			if err != nil {
				return
			}
			if msg == nil {
				return
			}
			select {
			case msgQueue <- msg:
			case <-ctx.Done():
				return
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
					Topic:     req.Topic,
					Key:       msg.Key,
					Value:     msg.Value,
					Headers:   msg.Headers,
					Partition: msg.Partition,
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

	elapsed := time.Since(start)
	result := &BatchResult{
		Produced:  atomic.LoadInt64(&produced),
		Failed:    atomic.LoadInt64(&failed),
		Cancelled: cancelled.Load(),
		ElapsedMs: elapsed.Milliseconds(),
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
	Key       []byte
	Value     []byte
	Headers   []kgo.RecordHeader
	Partition int32
}

func callGenerate(vm *goja.Runtime, generate goja.Callable, index int) (*genMessage, error) {
	ctxObj := vm.NewObject()
	ctxObj.Set("index", index)
	ctxObj.Set("time", time.Now().UnixMilli())
	ctxObj.Set("random", func() float64 {
		return float64(time.Now().UnixNano()%1e9) / 1e9
	})
	ctxObj.Set("uuid", func() string {
		return fmt.Sprintf("uuid-%d-%d", index, time.Now().UnixNano())
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
		return obj.Get(keys[index%len(keys)])
	})

	val, err := generate(goja.Undefined(), ctxObj)
	if err != nil {
		return nil, err
	}
	if val == nil || goja.IsUndefined(val) || goja.IsNull(val) {
		return nil, nil
	}

	return parseGenMessage(vm, val)
}

func parseGenMessage(vm *goja.Runtime, val goja.Value) (*genMessage, error) {
	obj := val.ToObject(vm)
	if obj == nil {
		return &genMessage{
			Value: []byte(val.String()),
		}, nil
	}

	msg := &genMessage{Partition: -1}

	if keyVal := obj.Get("key"); keyVal != nil && !goja.IsUndefined(keyVal) && !goja.IsNull(keyVal) {
		msg.Key = []byte(keyVal.String())
	}
	if valVal := obj.Get("value"); valVal != nil && !goja.IsUndefined(valVal) && !goja.IsNull(valVal) {
		msg.Value = []byte(valVal.String())
	}
	if partVal := obj.Get("partition"); partVal != nil && !goja.IsUndefined(partVal) {
		msg.Partition = int32(partVal.ToInteger())
	}
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

	return msg, nil
}

func msgToSample(msg *genMessage) Sample {
	s := Sample{
		Value:     string(msg.Value),
		Partition: msg.Partition,
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
