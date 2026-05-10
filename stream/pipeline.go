package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RouteFunc[T any] func(msg Message[T]) int

type Pipeline[T any] struct {
	source       Source[T]
	sinks        []Sink[T]
	dlq          Sink[T]
	transforms   []TransformFunc[T]
	flatMaps     []FlatMapFunc[T]
	retryCfg     RetryConfig
	batchCfg     BatchConfig
	windowCfg    WindowConfig
	backpressure *Backpressure
	sinkBP       []*Backpressure
	bpCfg        *BackpressureConfig
	metrics      Metrics
	errorHandler ErrorHandler
	state        State
	workers      int
	routes       []RouteFunc[T]
	routeSinks   [][]Sink[T]

	slideNext time.Time
}

func NewPipeline[T any](source Source[T], sink Sink[T]) *Pipeline[T] {
	return &Pipeline[T]{
		source:   source,
		sinks:    []Sink[T]{sink},
		retryCfg: DefaultRetryConfig(),
		metrics:  NoopMetrics{},
		workers:  1,
	}
}

func (p *Pipeline[T]) WithRetryConfig(cfg RetryConfig) *Pipeline[T] {
	p.retryCfg = cfg
	return p
}

func (p *Pipeline[T]) WithBackpressure(cfg BackpressureConfig) *Pipeline[T] {
	if err := cfg.Validate(); err != nil {
		panic(fmt.Errorf("pipeline: invalid backpressure config: %w", err))
	}
	bp, err := NewBackpressure(cfg)
	if err != nil {
		panic(fmt.Errorf("pipeline: invalid backpressure config: %w", err))
	}
	p.backpressure = bp
	p.bpCfg = &cfg
	return p
}

func (p *Pipeline[T]) WithBatchConfig(cfg BatchConfig) *Pipeline[T] {
	if err := cfg.Validate(); err != nil {
		panic(fmt.Errorf("pipeline: invalid batch config: %w", err))
	}
	p.batchCfg = cfg
	return p
}

func (p *Pipeline[T]) WithMetrics(m Metrics) *Pipeline[T] {
	if m == nil {
		p.metrics = NoopMetrics{}
	} else {
		p.metrics = m
	}
	return p
}

func (p *Pipeline[T]) WithDLQ(sink Sink[T]) *Pipeline[T] {
	p.dlq = sink
	return p
}

func (p *Pipeline[T]) WithErrorHandler(h ErrorHandler) *Pipeline[T] {
	p.errorHandler = h
	return p
}

func (p *Pipeline[T]) WithState(state State) *Pipeline[T] {
	p.state = state
	return p
}

func (p *Pipeline[T]) WithWindow(cfg WindowConfig) *Pipeline[T] {
	if err := cfg.Validate(); err != nil {
		panic(fmt.Errorf("pipeline: invalid window config: %w", err))
	}
	p.windowCfg = cfg
	return p
}

func (p *Pipeline[T]) WithWorkers(n int) *Pipeline[T] {
	if n < 1 {
		n = 1
	}
	p.workers = n
	return p
}

func (p *Pipeline[T]) AddSink(sink Sink[T]) *Pipeline[T] {
	p.sinks = append(p.sinks, sink)
	return p
}

func (p *Pipeline[T]) AddTransform(fn TransformFunc[T]) *Pipeline[T] {
	p.transforms = append(p.transforms, fn)
	return p
}

func (p *Pipeline[T]) Filter(fn func(ctx context.Context, msg Message[T]) (bool, error)) *Pipeline[T] {
	p.transforms = append(p.transforms, NewFilter(fn))
	return p
}

func (p *Pipeline[T]) FlatMap(fn FlatMapFunc[T]) *Pipeline[T] {
	p.flatMaps = append(p.flatMaps, fn)
	return p
}

func (p *Pipeline[T]) Split(routes []RouteFunc[T], sinks [][]Sink[T]) *Pipeline[T] {
	p.routes = routes
	p.routeSinks = sinks
	return p
}

func (p *Pipeline[T]) processTransforms(ctx context.Context, msg Message[T]) ([]Message[T], error) {
	for _, tf := range p.transforms {
		var err error
		msg, err = tf(ctx, msg)
		if err != nil {
			if isFilterSkip(err) {
				return nil, ErrSkip
			}
			return nil, err
		}
	}
	if len(p.flatMaps) > 0 {
		var all []Message[T]
		current := []Message[T]{msg}
		for _, fm := range p.flatMaps {
			var next []Message[T]
			for _, m := range current {
				out, err := fm(ctx, m)
				if err != nil {
					return nil, err
				}
				next = append(next, out...)
			}
			current = next
		}
		all = current
		return all, nil
	}
	return []Message[T]{msg}, nil
}

func (p *Pipeline[T]) Run(ctx context.Context) error {
	trackedSrc := NewTrackedLifecycle(lifecycleFromSource(p.source))
	trackedSinks := make([]*TrackedLifecycle, len(p.sinks))
	for i, s := range p.sinks {
		trackedSinks[i] = NewTrackedLifecycle(lifecycleFromSink(s))
	}

	src := p.source
	snks := p.sinks

	if _, ok := p.metrics.(NoopMetrics); !ok {
		src = NewInstrumentedSource(src, p.metrics, "")
		wrapped := make([]Sink[T], len(snks))
		for i, snk := range snks {
			wrapped[i] = NewInstrumentedSink(snk, p.metrics, "")
		}
		snks = wrapped
	}

	if p.bpCfg != nil && p.bpCfg.PerSink {
		p.sinkBP = make([]*Backpressure, len(snks))
		for i := range snks {
			bp, err := NewBackpressure(*p.bpCfg)
			if err != nil {
				return fmt.Errorf("pipeline: per-sink backpressure: %w", err)
			}
			p.sinkBP[i] = bp
		}
	}

	if err := trackedSrc.Open(ctx); err != nil {
		return fmt.Errorf("pipeline: source open: %w", err)
	}

	var openErrs []error
	for i, ts := range trackedSinks {
		if err := ts.Open(ctx); err != nil {
			openErrs = append(openErrs, fmt.Errorf("pipeline: sink[%d] open: %w", i, err))
		}
	}
	if len(openErrs) > 0 {
		trackedSrc.Close(ctx)
		for _, ts := range trackedSinks {
			ts.Close(ctx)
		}
		return errors.Join(openErrs...)
	}

	err := p.runLoop(ctx, src, snks)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, snk := range snks {
		snk.Flush(closeCtx)
	}
	for _, ts := range trackedSinks {
		if cerr := ts.Close(closeCtx); cerr != nil && err == nil {
			err = fmt.Errorf("pipeline: sink close: %w", cerr)
		}
	}
	if cerr := trackedSrc.Close(closeCtx); cerr != nil && err == nil {
		err = fmt.Errorf("pipeline: source close: %w", cerr)
	}
	if p.backpressure != nil {
		p.backpressure.Close()
	}
	for _, bp := range p.sinkBP {
		bp.Close()
	}
	return err
}

func (p *Pipeline[T]) runLoop(ctx context.Context, src Source[T], snks []Sink[T]) error {
	if p.workers <= 1 {
		return p.runLoopSingle(ctx, src, snks)
	}
	return p.runLoopConcurrent(ctx, src, snks)
}

func (p *Pipeline[T]) runLoopSingle(ctx context.Context, src Source[T], snks []Sink[T]) error {
	return p.runLoopInternal(ctx, src, snks)
}

func (p *Pipeline[T]) runLoopConcurrent(ctx context.Context, src Source[T], snks []Sink[T]) error {
	type workItem struct {
		msg Message[T]
		err error
	}
	work := make(chan workItem, p.workers)
	done := make(chan struct{}, p.workers)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	reader := func() {
		defer close(work)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg, err := src.Read(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					if p.errorHandler != nil {
						p.errorHandler.OnReadError(ctx, err)
					}
				}
				return
			}
			select {
			case work <- workItem{msg: msg}:
			case <-ctx.Done():
				return
			}
		}
	}

	worker := func() {
		defer func() { done <- struct{}{} }()
		for item := range work {
			if item.err != nil {
				continue
			}
			p.processAndWrite(ctx, snks, item.msg)
		}
	}

	go reader()
	for i := 0; i < p.workers; i++ {
		go worker()
	}

	<-ctx.Done()
	for i := 0; i < p.workers; i++ {
		<-done
	}
	return ctx.Err()
}

func (p *Pipeline[T]) runLoopInternal(ctx context.Context, src Source[T], snks []Sink[T]) error {
	var batch []Message[T]
	var windowBuf []Message[T]
	var flushTick *time.Ticker
	useBatch := p.batchCfg.Size > 0 || p.batchCfg.Interval > 0
	useWindow := p.windowCfg.Size > 0

	if useBatch && p.batchCfg.Interval > 0 {
		flushTick = time.NewTicker(p.batchCfg.Interval)
		defer flushTick.Stop()
	}

	for {
		if flushTick != nil {
			select {
			case <-flushTick.C:
				p.flushBatch(ctx, snks, batch)
				batch = batch[:0]
			default:
			}
		}

		select {
		case <-ctx.Done():
			if useBatch {
				p.flushBatch(ctx, snks, batch)
			}
			if useWindow && len(windowBuf) > 0 {
				p.emitWindow(ctx, snks, windowBuf)
			}
			return ctx.Err()
		default:
		}

		if p.backpressure != nil {
			start := time.Now()
			if err := p.backpressure.Wait(ctx); err != nil {
				return err
			}
			p.metrics.BackpressureWait("", time.Since(start))
		}

		var msg Message[T]
		err := DoWithRetry(ctx, func(ctx context.Context) error {
			var err error
			msg, err = src.Read(ctx)
			return err
		}, p.retryCfg)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if p.errorHandler != nil {
				p.errorHandler.OnReadError(ctx, err)
			}
			continue
		}

		results, err := p.processTransforms(ctx, msg)
		if err != nil {
			if isFilterSkip(err) {
				continue
			}
			p.metrics.TransformError("")
			if p.errorHandler != nil {
				p.errorHandler.OnTransformError(ctx, convertMsg(msg), err)
			}
			msg.Nack(ctx)
			continue
		}

		for _, out := range results {
			if useWindow {
				windowBuf = append(windowBuf, out)
				p.processWindow(ctx, snks, &windowBuf)
				continue
			}
			if useBatch {
				batch = append(batch, out)
				if p.batchCfg.Size > 0 && len(batch) >= p.batchCfg.Size {
					p.flushBatch(ctx, snks, batch)
					batch = batch[:0]
				}
			} else {
				p.writeToSinks(ctx, snks, out)
			}
		}
	}
}

func (p *Pipeline[T]) processWindow(ctx context.Context, snks []Sink[T], buf *[]Message[T]) {
	if len(*buf) == 0 {
		return
	}

	switch p.windowCfg.Type {
	case WindowTypeCount:
		if len(*buf) >= p.windowCfg.Count {
			p.emitWindow(ctx, snks, *buf)
			*buf = nil
		}

	case WindowTypeSession:
		last := (*buf)[len(*buf)-1]
		if len(*buf) > 1 {
			prev := (*buf)[len(*buf)-2]
			if !last.Timestamp.IsZero() && !prev.Timestamp.IsZero() {
				gap := last.Timestamp.Sub(prev.Timestamp)
				if gap > p.windowCfg.SessionGap {
					session := (*buf)[:len(*buf)-1]
					p.emitWindow(ctx, snks, session)
					*buf = (*buf)[len(*buf)-1:]
				}
			}
		}

	case WindowTypeSliding:
		if p.slideNext.IsZero() && len(*buf) > 0 {
			p.slideNext = (*buf)[0].Timestamp.Truncate(p.windowCfg.Slide).Add(p.windowCfg.Slide)
		}
		last := (*buf)[len(*buf)-1]
		for !p.slideNext.IsZero() && !last.Timestamp.Before(p.slideNext) {
			var window []Message[T]
			cutoff := p.slideNext
			for _, msg := range *buf {
				if !msg.Timestamp.Before(cutoff.Add(-p.windowCfg.Size)) && msg.Timestamp.Before(cutoff) {
					window = append(window, msg)
				}
			}
			if len(window) > 0 {
				p.emitWindow(ctx, snks, window)
			}
			p.slideNext = p.slideNext.Add(p.windowCfg.Slide)
			removeBefore := p.slideNext.Add(-p.windowCfg.Size)
			var keep []Message[T]
			for _, msg := range *buf {
				if msg.Timestamp.After(removeBefore) || msg.Timestamp.Equal(removeBefore) {
					keep = append(keep, msg)
				}
			}
			*buf = keep
		}

	default:
		first := (*buf)[0]
		last := (*buf)[len(*buf)-1]
		windowEnd := first.Timestamp.Truncate(p.windowCfg.Size).Add(p.windowCfg.Size)
		if last.Timestamp.After(windowEnd) || last.Timestamp.Equal(windowEnd) {
			p.emitWindow(ctx, snks, *buf)
			*buf = nil
		}
	}
}

func (p *Pipeline[T]) emitWindow(ctx context.Context, snks []Sink[T], msgs []Message[T]) {
	for _, msg := range msgs {
		p.writeToSinks(ctx, snks, msg)
	}
}

func (p *Pipeline[T]) processAndWrite(ctx context.Context, snks []Sink[T], msg Message[T]) {
	results, err := p.processTransforms(ctx, msg)
	if err != nil {
		if isFilterSkip(err) {
			return
		}
		p.metrics.TransformError("")
		if p.errorHandler != nil {
			p.errorHandler.OnTransformError(ctx, convertMsg(msg), err)
		}
		msg.Nack(ctx)
		return
	}
	for _, out := range results {
		p.writeToSinks(ctx, snks, out)
	}
}

func (p *Pipeline[T]) writeToSinks(ctx context.Context, snks []Sink[T], msg Message[T]) {
	if len(p.routes) > 0 {
		p.writeWithRouting(ctx, msg)
		return
	}

	var failed bool
	for i, snk := range snks {
		if i < len(p.sinkBP) && p.sinkBP[i] != nil {
			if err := p.sinkBP[i].Wait(ctx); err != nil {
				return
			}
		}
		err := DoWithRetry(ctx, func(ctx context.Context) error {
			return snk.Write(ctx, msg)
		}, p.retryCfg)
		if err != nil {
			p.metrics.MessageFailed("", err)
			if p.errorHandler != nil {
				p.errorHandler.OnWriteError(ctx, convertMsg(msg), err)
			}
			failed = true
		}
	}

	for _, snk := range snks {
		snk.Flush(ctx)
	}

	if failed {
		msg.Nack(ctx)
		if p.dlq != nil {
			p.dlq.Write(ctx, msg)
		}
	} else {
		msg.Ack(ctx)
	}
}

func (p *Pipeline[T]) writeWithRouting(ctx context.Context, msg Message[T]) {
	for i, route := range p.routes {
		idx := route(msg)
		if idx >= 0 && idx < len(p.routeSinks) && i < len(p.routeSinks[idx]) {
			snk := p.routeSinks[idx][i]
			err := DoWithRetry(ctx, func(ctx context.Context) error {
				return snk.Write(ctx, msg)
			}, p.retryCfg)
			if err != nil {
				p.metrics.MessageFailed("", err)
				if p.errorHandler != nil {
					p.errorHandler.OnWriteError(ctx, convertMsg(msg), err)
				}
			}
		}
	}
}

func (p *Pipeline[T]) flushBatch(ctx context.Context, snks []Sink[T], batch []Message[T]) {
	for _, msg := range batch {
		p.writeToSinks(ctx, snks, msg)
	}
}

func lifecycleFromSource[T any](s Source[T]) Lifecycle {
	return &LifecycleFunc{
		OpenFn:  func(ctx context.Context) error { return s.Open(ctx) },
		CloseFn: func(ctx context.Context) error { return s.Close(ctx) },
	}
}

func lifecycleFromSink[T any](s Sink[T]) Lifecycle {
	return &LifecycleFunc{
		OpenFn:  func(ctx context.Context) error { return s.Open(ctx) },
		CloseFn: func(ctx context.Context) error { return s.Close(ctx) },
	}
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func convertMsg[T any](msg Message[T]) Message[[]byte] {
	return Message[[]byte]{
		Key:   msg.Key,
		Value: nil,
	}
}
