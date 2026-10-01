package runtime

import (
	"sync"
	"time"
)

// ExecutorConfig 并发与超时配置。
type ExecutorConfig struct {
	MaxConcurrent int           // 每工具并发上限
	QueueSize     int           // 每工具等待队列容量
	CallTimeout   time.Duration // 单次执行超时
	MaxRetries    int           // 失败重试次数（重试须幂等）
}

// CallContext 传给工具桩的上下文。
type CallContext struct {
	CallID  string
	Tool    string
	Version int
	Attempt int
	Done    <-chan struct{} // 取消/超时信号
}

// lane 单个工具的并发通道：sem 控制并行度，queue 是有界等待队列。
type lane struct {
	sem   chan struct{}
	queue chan struct{}
}

// executor 按工具隔离并发，互不影响。
type executor struct {
	cfg   ExecutorConfig
	mu    sync.Mutex
	lanes map[string]*lane
}

func newExecutor(cfg ExecutorConfig) *executor {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	return &executor{cfg: cfg, lanes: make(map[string]*lane)}
}

func (e *executor) laneFor(tool string) *lane {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, ok := e.lanes[tool]
	if !ok {
		l = &lane{
			sem:   make(chan struct{}, e.cfg.MaxConcurrent),
			queue: make(chan struct{}, e.cfg.QueueSize),
		}
		e.lanes[tool] = l
	}
	return l
}

// acquire 尝试获取执行槽位。
// 成功返回 release；并发满且队列满返回可区分的拒绝。
// cancel 非 nil 时，排队中收到取消信号返回 KindCanceled。
func (e *executor) acquire(tool string, cancel <-chan struct{}) (func(), *RejectError) {
	l := e.laneFor(tool)
	// 快路径：直接拿到执行位
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem }, nil
	default:
	}
	// 进入有界队列等待执行位
	select {
	case l.queue <- struct{}{}:
	default:
		if e.cfg.QueueSize == 0 {
			return nil, reject(KindConcurrencyFull, "", "tool %q concurrency limit %d reached", tool, e.cfg.MaxConcurrent)
		}
		return nil, reject(KindQueueFull, "", "tool %q queue full (size %d)", tool, e.cfg.QueueSize)
	}
	// 排队等待执行位，支持取消
	select {
	case l.sem <- struct{}{}:
		<-l.queue
		return func() { <-l.sem }, nil
	case <-cancel:
		<-l.queue
		return nil, reject(KindCanceled, "", "canceled while queued for tool %q", tool)
	}
}
