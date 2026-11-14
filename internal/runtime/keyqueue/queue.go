// Package keyqueue provides a per-partition-key ordered queue with bounded concurrency.
// It preserves ordering for events sharing a partition key while limiting global in-flight work.
package keyqueue

import (
	"context"
	"errors"
	"fmt"
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
	drainOnce      sync.Once
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
func NewKeyQueue(maxGlobalInFlight int, maxPerKeyQueue int, workerCount int, processor func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error, clock func() time.Time, hotKeyThreshold int, hotKeyCheckInterval time.Duration, hotKeyCallback func(partitionKey string, depth int)) *KeyQueue {
	kq := &KeyQueue{maxGlobalInFlight: maxGlobalInFlight, maxPerKey: maxPerKeyQueue,
		workerCount: workerCount, processor: processor, clock: clock,
		hotKeyThreshold: hotKeyThreshold, hotKeyCheckInterval: hotKeyCheckInterval, hotKeyCallback: hotKeyCallback}
	kq.normalizeCapacity()
	kq.initializeHotKeys()
	kq.queues = make(map[string]*keyQueue)
	kq.globalSem = make(chan struct{}, kq.maxGlobalInFlight)
	kq.stopWorkers = make(chan struct{})
	kq.stopSubmitting = make(chan struct{})
	return kq
}

// Start launches the queue workers once. A stopped KeyQueue cannot be restarted.
func (kq *KeyQueue) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("keyqueue start: context is required")
	}
	kq.lifecycleMu.Lock()
	defer kq.lifecycleMu.Unlock()
	if kq.started || kq.stopped {
		return nil
	}
	kq.started = true
	kq.startLoops(ctx)
	return nil
}

// Stop asks all queue goroutines to stop and waits for them. It is safe to call
// Stop more than once.
func (kq *KeyQueue) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("keyqueue stop: context is required")
	}
	kq.stopOnce.Do(kq.stopSubmissionsAndWorkers)
	kq.wg.Wait()
	kq.wgHot.Wait()
	kq.drainOnce.Do(func() { kq.drainPending(ctx) })
	return nil
}

// drainPending hands queued, unprocessed deliveries back to the broker after
// workers stop; otherwise they wait for AckWait while this pod releases leases.
func (kq *KeyQueue) drainPending(ctx context.Context) {
	kq.mu.Lock()
	queues := make([]*keyQueue, 0, len(kq.queues))
	for _, queue := range kq.queues {
		queues = append(queues, queue)
	}
	kq.mu.Unlock()
	for _, queue := range queues {
		kq.drainQueue(ctx, queue)
	}
}

func (kq *KeyQueue) drainQueue(ctx context.Context, queue *keyQueue) {
	for {
		select {
		case item := <-queue.ch:
			kq.retryUnprocessed(ctx, item)
			<-kq.globalSem
		default:
			return
		}
	}
}

func (kq *KeyQueue) retryUnprocessed(ctx context.Context, item *queuedItem) {
	if item == nil || item.delivery == nil {
		return
	}
	if err := item.delivery.Retry(ctx, time.Second); err != nil {
		log.Printf("retry queued delivery on shutdown: %v", err)
	}
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
		if kq.updateHotKeyLocked(key, depth, now) {
			notices = append(notices, hotKeyNotice{key: key, depth: depth})
		}
	}
	kq.removeInactiveHotKeysLocked(queues)
	return notices
}

func (kq *KeyQueue) notifyHotKeys(notices []hotKeyNotice) {
	// Callbacks run on the owned checker goroutine; implementations should be quick.
	for _, notice := range notices {
		kq.hotKeyCallback(notice.key, notice.depth)
	}
}

// queueIdentity uses the tenant and partition key to avoid cross-tenant blocking.
// Prefixing the tenant length makes concatenated identities unambiguous.
func queueIdentity(env *domain.EventEnvelope) string {
	return fmt.Sprintf("%d:%s%s", len(env.TenantID), env.TenantID, env.PartitionKey)
}

// ErrQueueFull signals bounded admission failure; callers should retry with delay.
var ErrQueueFull = errors.New("key queue is at capacity")

// TrySubmit never waits for a busy key or exhausted global capacity.
// A hot key can occupy at most half the available global slots.
func (kq *KeyQueue) TrySubmit(ctx context.Context, item ports.QueueItem) error {
	if err := validateQueueSubmission(ctx, item); err != nil {
		return err
	}
	if err := kq.beginSubmission(); err != nil {
		return err
	}
	defer kq.submitWG.Done()
	queue := kq.registerQueueSubmitter(queueIdentity(item.Event))
	defer kq.finishSubmit(queue)
	return kq.tryEnqueue(ctx, queue, item)
}

func (kq *KeyQueue) tryEnqueue(ctx context.Context, queue *keyQueue, item ports.QueueItem) error {
	if err := kq.acquireSlotWithoutWaiting(ctx); err != nil {
		return err
	}
	if err := kq.tryEnqueueWithSlot(queue, item); err != nil {
		<-kq.globalSem
		return err
	}
	return nil
}

func (kq *KeyQueue) acquireSlotWithoutWaiting(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-kq.stopSubmitting:
		return errors.New("submit event: key queue is stopping")
	case kq.globalSem <- struct{}{}:
		return nil
	default:
		return ErrQueueFull
	}
}

func (kq *KeyQueue) tryEnqueueWithSlot(q *keyQueue, item ports.QueueItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ch) >= kq.fairKeyLimit() {
		return ErrQueueFull
	}
	queued := &queuedItem{env: item.Event, delivery: item.Delivery, fencingToken: item.FencingToken, vshard: item.Shard, workerID: item.WorkerID}
	select {
	case q.ch <- queued:
		return nil
	default:
		return ErrQueueFull
	}
}

func (kq *KeyQueue) fairKeyLimit() int {
	return min(kq.maxPerKey, max(1, kq.maxGlobalInFlight/2))
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
	queue := kq.registerQueueSubmitter(queueIdentity(item.Event))
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
		queue = newPartitionQueue(partitionKey, kq.maxPerKey)
		kq.queues[partitionKey] = queue
	}
	queue.mu.Lock()
	queue.submitters++
	queue.mu.Unlock()
	return queue
}

func (kq *KeyQueue) enqueueItem(ctx context.Context, queue *keyQueue, item ports.QueueItem) error {
	defer kq.finishSubmit(queue)
	queued := &queuedItem{env: item.Event, delivery: item.Delivery, fencingToken: item.FencingToken, vshard: item.Shard, workerID: item.WorkerID}
	select {
	case queue.ch <- queued:
		return nil
	case <-ctx.Done():
		<-kq.globalSem
		return ctx.Err()
	case <-kq.stopSubmitting:
		<-kq.globalSem
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
	for kq.workerShouldContinue(ctx) {
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
	kq.finishEvent(queueIdentity(item.env))
	<-kq.globalSem
}

func (kq *KeyQueue) nextEvent() *queuedItem {
	selected := kq.selectReadyQueue()
	if selected == nil {
		return nil
	}
	// Keep processing true until the processor finishes, preserving per-key exclusion.
	return <-selected.ch
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
	stats := ports.QueueStats{ActiveKeys: len(kq.queues), MaxGlobalDepth: kq.maxGlobalInFlight}
	for _, q := range kq.queues {
		stats.GlobalDepth += len(q.ch)
		if len(q.ch) > 0 && !q.processing {
			stats.Processing++
		}
	}
	stats.HotKeys = kq.hotKeyStatsLocked()
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

func (kq *KeyQueue) normalizeCapacity() {
	if kq.maxGlobalInFlight <= 0 {
		kq.maxGlobalInFlight = 100
	}
	if kq.maxPerKey <= 0 {
		kq.maxPerKey = 100
	}
	if kq.workerCount <= 0 {
		kq.workerCount = 1
	}
	if kq.clock == nil {
		kq.clock = time.Now
	}
}

func (kq *KeyQueue) initializeHotKeys() {
	if kq.hotKeyThreshold <= 0 {
		kq.hotKeyThreshold = 50
	}
	if kq.hotKeyThreshold > kq.maxPerKey {
		kq.hotKeyThreshold = kq.maxPerKey
	}
	if kq.hotKeyCheckInterval <= 0 {
		kq.hotKeyCheckInterval = 10 * time.Second
	}
	kq.hotKeys = make(map[string]time.Time)
	kq.stopHotKeyCheck = make(chan struct{})
}

func (kq *KeyQueue) startLoops(ctx context.Context) {
	for i := 0; i < kq.workerCount; i++ {
		kq.wg.Add(1)
		go kq.workerLoop(ctx, i)
	}
	if kq.hotKeyCallback != nil {
		kq.wgHot.Add(1)
		go kq.hotKeyCheckLoop(ctx)
	}
}

func (kq *KeyQueue) stopSubmissionsAndWorkers() {
	kq.lifecycleMu.Lock()
	kq.stopped = true
	close(kq.stopSubmitting)
	kq.lifecycleMu.Unlock()
	// Drain admitted submissions while workers remain alive.
	kq.submitWG.Wait()
	close(kq.stopWorkers)
	close(kq.stopHotKeyCheck)
}

func (kq *KeyQueue) updateHotKeyLocked(key string, depth int, now time.Time) bool {
	if depth < kq.hotKeyThreshold {
		delete(kq.hotKeys, key)
		return false
	}
	if _, tracked := kq.hotKeys[key]; tracked {
		return false
	}
	kq.hotKeys[key] = now
	return true
}

func (kq *KeyQueue) removeInactiveHotKeysLocked(queues map[string]*keyQueue) {
	for key := range kq.hotKeys {
		if _, exists := queues[key]; !exists {
			delete(kq.hotKeys, key)
		}
	}
}

func (kq *KeyQueue) selectReadyQueue() *keyQueue {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	for _, q := range kq.queues {
		if q.acquireProcessing() {
			return q
		}
	}
	return nil
}

func (q *keyQueue) acquireProcessing() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.processing || len(q.ch) == 0 {
		return false
	}
	q.processing = true
	return true
}

func (kq *KeyQueue) hotKeyStatsLocked() []ports.HotKeyInfo {
	kq.muHot.Lock()
	defer kq.muHot.Unlock()
	var keys []ports.HotKeyInfo
	for key, since := range kq.hotKeys {
		keys = append(keys, ports.HotKeyInfo{PartitionKey: key, Depth: len(kq.queues[key].ch), Since: since})
	}
	return keys
}

func newPartitionQueue(partitionKey string, capacity int) *keyQueue {
	return &keyQueue{ch: make(chan *queuedItem, capacity), partitionKey: partitionKey}
}
