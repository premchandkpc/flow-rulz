// Package keyqueue provides a per-partition-key ordered queue with bounded concurrency.
// It preserves ordering for events sharing a partition key while limiting global in-flight work.
package keyqueue

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// KeyQueue is a per-partition-key ordered queue with bounded concurrency.
// It preserves ordering for events sharing a partition key while limiting global in-flight work.
type KeyQueue struct {
	mu             sync.Mutex
	queues         map[string]*keyQueue
	lifecycleMu    sync.Mutex
	started        bool
	stopped        bool
	stopOnce       sync.Once
	submitWG       sync.WaitGroup
	maxPerKey      int
	globalSem      chan struct{}
	workerCount    int
	stopWorkers    chan struct{}
	stopSubmitting chan struct{}
	wg             sync.WaitGroup
	processor      func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error
	clock          func() time.Time

	// Hot-key detection
	hotKeyThreshold     int
	hotKeyCheckInterval time.Duration
	hotKeyCallback      func(partitionKey string, depth int)
	muHot               sync.Mutex
	hotKeys             map[string]time.Time // key -> first detected time
	stopHotKeyCheck     chan struct{}
	wgHot               sync.WaitGroup

	// Stats
	maxGlobalInFlight int
}

type keyQueue struct {
	ch           chan *queuedItem
	partitionKey string
	processing   bool
	submitters   int
	mu           sync.Mutex
}

type queuedItem struct {
	env          *domain.EventEnvelope
	delivery     ports.Delivery
	fencingToken int64
	vshard       uint32
	workerID     string
}

// NewKeyQueue creates a new KeyQueue with the given configuration.
func NewKeyQueue(
	maxGlobalInFlight int,
	maxPerKeyQueue int,
	workerCount int,
	processor func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error,
	clock func() time.Time,
	hotKeyThreshold int,
	hotKeyCheckInterval time.Duration,
	hotKeyCallback func(partitionKey string, depth int),
) *KeyQueue {
	if maxGlobalInFlight <= 0 {
		maxGlobalInFlight = 100
	}
	if maxPerKeyQueue <= 0 {
		maxPerKeyQueue = 100
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	if clock == nil {
		clock = time.Now
	}
	if hotKeyThreshold <= 0 {
		hotKeyThreshold = 1000
	}
	if hotKeyCheckInterval <= 0 {
		hotKeyCheckInterval = 10 * time.Second
	}
	return &KeyQueue{
		queues:              make(map[string]*keyQueue),
		maxPerKey:           maxPerKeyQueue,
		globalSem:           make(chan struct{}, maxGlobalInFlight),
		workerCount:         workerCount,
		stopWorkers:         make(chan struct{}),
		stopSubmitting:      make(chan struct{}),
		processor:           processor,
		clock:               clock,
		hotKeyThreshold:     hotKeyThreshold,
		hotKeyCheckInterval: hotKeyCheckInterval,
		hotKeyCallback:      hotKeyCallback,
		hotKeys:             make(map[string]time.Time),
		stopHotKeyCheck:     make(chan struct{}),
		maxGlobalInFlight:   maxGlobalInFlight,
	}
}

// Start launches the queue workers once. A stopped KeyQueue cannot be restarted.
func (kq *KeyQueue) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("keyqueue start: context is required")
	}
	kq.lifecycleMu.Lock()
	if kq.started || kq.stopped {
		kq.lifecycleMu.Unlock()
		return nil
	}
	kq.started = true
	for i := 0; i < kq.workerCount; i++ {
		kq.wg.Add(1)
		go kq.workerLoop(ctx, i)
	}
	if kq.hotKeyCallback != nil {
		kq.wgHot.Add(1)
		go kq.hotKeyCheckLoop(ctx)
	}
	kq.lifecycleMu.Unlock()
	return nil
}

// Stop asks all queue goroutines to stop and waits for them. It is safe to call
// Stop more than once.
func (kq *KeyQueue) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("keyqueue stop: context is required")
	}
	kq.stopOnce.Do(func() {
		kq.lifecycleMu.Lock()
		kq.stopped = true
		close(kq.stopSubmitting)
		kq.lifecycleMu.Unlock()

		// Let submissions already admitted finish before closing the worker
		// signal channels. Workers remain alive while these submissions drain.
		kq.submitWG.Wait()
		close(kq.stopWorkers)
		close(kq.stopHotKeyCheck)
	})
	kq.wg.Wait()
	kq.wgHot.Wait()
	return nil
}

func (kq *KeyQueue) hotKeyCheckLoop(ctx context.Context) {
	defer kq.wgHot.Done()
	ticker := time.NewTicker(kq.hotKeyCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-kq.stopHotKeyCheck:
			return
		case <-ticker.C:
			kq.checkHotKeys()
		}
	}
}

type hotKeyNotice struct {
	key   string
	depth int
}

func (kq *KeyQueue) checkHotKeys() {
	queues := kq.queueSnapshot()
	notices := kq.updateHotKeyState(queues, kq.clock())
	kq.notifyHotKeys(notices)
}

func (kq *KeyQueue) queueSnapshot() map[string]*keyQueue {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	queues := make(map[string]*keyQueue, len(kq.queues))
	for key, queue := range kq.queues {
		queues[key] = queue
	}
	return queues
}

func (kq *KeyQueue) updateHotKeyState(queues map[string]*keyQueue, now time.Time) []hotKeyNotice {
	var notices []hotKeyNotice
	kq.muHot.Lock()
	defer kq.muHot.Unlock()
	for key, queue := range queues {
		depth := len(queue.ch)
		if depth >= kq.hotKeyThreshold {
			if _, tracked := kq.hotKeys[key]; !tracked {
				kq.hotKeys[key] = now
				notices = append(notices, hotKeyNotice{key: key, depth: depth})
			}
		} else {
			delete(kq.hotKeys, key)
		}
	}
	for key := range kq.hotKeys {
		if _, exists := queues[key]; !exists {
			delete(kq.hotKeys, key)
		}
	}
	return notices
}

func (kq *KeyQueue) notifyHotKeys(notices []hotKeyNotice) {
	// Callbacks run on the owned checker goroutine; implementations should be quick.
	for _, notice := range notices {
		kq.hotKeyCallback(notice.key, notice.depth)
	}
}

// Submit enqueues an item for processing, respecting per-key ordering and global concurrency limits.
func (kq *KeyQueue) Submit(ctx context.Context, item ports.QueueItem) error {
	if err := validateQueueSubmission(ctx, item); err != nil {
		return err
	}
	if err := kq.beginSubmission(); err != nil {
		return err
	}
	defer kq.submitWG.Done()

	if err := kq.acquireGlobalSlot(ctx); err != nil {
		return err
	}
	queue := kq.registerQueueSubmitter(item.Event.PartitionKey)
	return kq.enqueueItem(ctx, queue, item)
}

func validateQueueSubmission(ctx context.Context, item ports.QueueItem) error {
	if ctx == nil {
		return errors.New("submit event: context is nil")
	}
	if item.Event == nil {
		return errors.New("submit event: envelope is nil")
	}
	return nil
}

func (kq *KeyQueue) beginSubmission() error {
	kq.lifecycleMu.Lock()
	defer kq.lifecycleMu.Unlock()
	if !kq.started || kq.stopped {
		return errors.New("submit event: key queue is not running")
	}
	// Add under lifecycleMu so Stop cannot race Wait with a new Add.
	kq.submitWG.Add(1)
	return nil
}

func (kq *KeyQueue) acquireGlobalSlot(ctx context.Context) error {
	select {
	case kq.globalSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-kq.stopSubmitting:
		return errors.New("submit event: key queue is stopping")
	}
}

func (kq *KeyQueue) registerQueueSubmitter(partitionKey string) *keyQueue {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	queue, exists := kq.queues[partitionKey]
	if !exists {
		queue = &keyQueue{
			ch:           make(chan *queuedItem, kq.maxPerKey),
			partitionKey: partitionKey,
		}
		kq.queues[partitionKey] = queue
	}
	queue.mu.Lock()
	queue.submitters++
	queue.mu.Unlock()
	return queue
}

func (kq *KeyQueue) enqueueItem(ctx context.Context, queue *keyQueue, item ports.QueueItem) error {
	queued := &queuedItem{
		env: item.Event, delivery: item.Delivery, fencingToken: item.FencingToken,
		vshard: item.Shard, workerID: item.WorkerID,
	}
	select {
	case queue.ch <- queued:
		kq.finishSubmit(queue)
		return nil
	case <-ctx.Done():
		<-kq.globalSem
		kq.finishSubmit(queue)
		return ctx.Err()
	case <-kq.stopSubmitting:
		<-kq.globalSem
		kq.finishSubmit(queue)
		return errors.New("submit event: key queue is stopping")
	}
}

func (kq *KeyQueue) finishSubmit(q *keyQueue) {
	kq.mu.Lock()
	q.mu.Lock()
	q.submitters--
	if q.submitters < 0 {
		q.submitters = 0
	}
	if kq.queues[q.partitionKey] == q && !q.processing && q.submitters == 0 && len(q.ch) == 0 {
		delete(kq.queues, q.partitionKey)
	}
	q.mu.Unlock()
	kq.mu.Unlock()
}

func (kq *KeyQueue) workerLoop(ctx context.Context, workerID int) {
	defer kq.wg.Done()
	for {
		if !kq.workerShouldContinue(ctx) {
			return
		}
		item := kq.nextEvent()
		if item == nil {
			if !waitForQueueWork(ctx, kq.stopWorkers) {
				return
			}
			continue
		}
		kq.processQueuedItem(ctx, workerID, item)
	}
}

func (kq *KeyQueue) workerShouldContinue(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-kq.stopWorkers:
		return false
	default:
		return true
	}
}

func waitForQueueWork(ctx context.Context, stop <-chan struct{}) bool {
	timer := time.NewTimer(10 * time.Millisecond)
	defer stopAndDrainTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func (kq *KeyQueue) processQueuedItem(ctx context.Context, workerID int, item *queuedItem) {
	if err := kq.processor(ctx, item.env, item.delivery, item.fencingToken, item.vshard, item.workerID); err != nil {
		log.Printf("keyqueue worker %d: process error for key %s: %v", workerID, item.env.PartitionKey, err)
	}
	kq.finishEvent(item.env.PartitionKey)
	<-kq.globalSem
}

func (kq *KeyQueue) nextEvent() *queuedItem {
	kq.mu.Lock()
	var selected *keyQueue
	for _, q := range kq.queues {
		q.mu.Lock()
		if !q.processing && len(q.ch) > 0 {
			selected = q
			q.processing = true
		}
		q.mu.Unlock()
		if selected != nil {
			break
		}
	}
	kq.mu.Unlock()

	if selected == nil {
		return nil
	}

	item := <-selected.ch
	// Keep selected.processing true until the processor has completed. Otherwise
	// another worker can dequeue the next event for this key concurrently.
	return item
}

func (kq *KeyQueue) finishEvent(partitionKey string) {
	kq.mu.Lock()
	q := kq.queues[partitionKey]
	if q == nil {
		kq.mu.Unlock()
		return
	}
	q.mu.Lock()
	q.processing = false
	if q.submitters == 0 && len(q.ch) == 0 {
		delete(kq.queues, partitionKey)
	}
	q.mu.Unlock()
	kq.mu.Unlock()
}

// Stats returns current queue statistics including depth, active keys, and hot keys.
func (kq *KeyQueue) Stats() ports.QueueStats {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	stats := ports.QueueStats{
		ActiveKeys:     len(kq.queues),
		GlobalDepth:    0,
		MaxGlobalDepth: kq.maxGlobalInFlight,
	}
	for _, q := range kq.queues {
		stats.GlobalDepth += len(q.ch)
		if len(q.ch) > 0 && !q.processing {
			stats.Processing++
		}
	}
	// Hot keys
	kq.muHot.Lock()
	for key, since := range kq.hotKeys {
		stats.HotKeys = append(stats.HotKeys, ports.HotKeyInfo{
			PartitionKey: key,
			Depth:        len(kq.queues[key].ch),
			Since:        since,
		})
	}
	kq.muHot.Unlock()
	return stats
}

// QueueDepth returns the number of queued items for a specific partition key.
func (kq *KeyQueue) QueueDepth(partitionKey string) int {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	if q, ok := kq.queues[partitionKey]; ok {
		return len(q.ch)
	}
	return 0
}

// ActiveKeys returns the number of partition keys currently having queued items.
func (kq *KeyQueue) ActiveKeys() int {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	return len(kq.queues)
}
