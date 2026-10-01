package runtime

import (
	"fmt"
	"sort"
	"sync"
)

// toolVersions 同一工具的全部历史版本，按 Version 升序。
type toolVersions struct {
	defs map[int]ToolDef
	fns  map[int]ToolFunc
}

// Registry 登记工具定义与本地桩，保留全部历史版本以支持版本绑定。
type Registry struct {
	mu    sync.RWMutex
	tools map[string]*toolVersions
}

func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]*toolVersions)}
}

// Register 登记新定义；同名工具的 Version 必须严格递增。
func (r *Registry) Register(def ToolDef, fn ToolFunc) error {
	if def.Name == "" {
		return fmt.Errorf("tool name required")
	}
	if fn == nil {
		return fmt.Errorf("tool func required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	tv, ok := r.tools[def.Name]
	if !ok {
		tv = &toolVersions{defs: make(map[int]ToolDef), fns: make(map[int]ToolFunc)}
		r.tools[def.Name] = tv
	}
	if len(tv.defs) > 0 {
		latest := tv.defs[latestVersion(tv)]
		if def.Version <= latest.Version {
			return fmt.Errorf("tool %q version %d must be > current %d", def.Name, def.Version, latest.Version)
		}
	}
	if def.Version < 1 {
		return fmt.Errorf("tool %q version must be >= 1", def.Name)
	}
	tv.defs[def.Version] = def
	tv.fns[def.Version] = fn
	return nil
}

func latestVersion(tv *toolVersions) int {
	vs := make([]int, 0, len(tv.defs))
	for v := range tv.defs {
		vs = append(vs, v)
	}
	sort.Ints(vs)
	return vs[len(vs)-1]
}

// Current 返回当前生效（最新）版本。
func (r *Registry) Current(name string) (ToolDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tv, ok := r.tools[name]
	if !ok || len(tv.defs) == 0 {
		return ToolDef{}, false
	}
	return tv.defs[latestVersion(tv)], true
}

// Version 返回指定版本定义与桩，供已绑定版本的调用使用。
func (r *Registry) Version(name string, version int) (ToolDef, ToolFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tv, ok := r.tools[name]
	if !ok {
		return ToolDef{}, nil, false
	}
	def, ok1 := tv.defs[version]
	fn, ok2 := tv.fns[version]
	return def, fn, ok1 && ok2
}
