package taskq

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/vmihailenco/taskq/v3/internal"
)

const numSelectedThreshold = 20

type perfProfile struct {
	start     time.Time
	processed int
	retries   int
	timing    time.Duration

	tps       float64
	errorRate float64
}

func (p *perfProfile) Reset(processed, retries int) {
	p.start = time.Now()
	p.processed = processed
	p.retries = retries
}

func (p *perfProfile) Update(processed, retries int, timing time.Duration) {
	processedDiff := processed - p.processed
	retriesDiff := retries - p.retries
	total := processedDiff + retriesDiff
	elapsed := time.Since(p.start)

	elapsedMS := float64(elapsed) / float64(time.Millisecond)
	p.tps = float64(processedDiff) / elapsedMS

	if total > 0 {
		p.errorRate = float64(retriesDiff) / float64(total)
	} else {
		p.errorRate = 0
	}

	p.timing = timing
}

func (p *perfProfile) TPS() float64 {
	return p.tps
}

func (p *perfProfile) ErrorRate() float64 {
	return p.errorRate
}

func (p *perfProfile) Timing() time.Duration {
	return p.timing
}

//------------------------------------------------------------------------------

type consumerConfig struct {
	NumFetcher int32
	NumWorker  int32

	perfProfile

	NumSelected int
	Score       float64
}

func newConsumerConfig(numFetcher, numWorker int32) *consumerConfig {
	return &consumerConfig{
		NumFetcher: numFetcher,
		NumWorker:  numWorker,
	}
}

func (cfg *consumerConfig) SetScore(score float64) {
	if cfg.Score == 0 {
		cfg.Score = score
	} else {
		cfg.Score = (cfg.Score + score) / 2
	}
}

func (cfg *consumerConfig) String() string {
	return fmt.Sprintf("fetchers=%d workers=%d tps=%f failure=%f timing=%s score=%f selected=%d",
		cfg.NumFetcher, cfg.NumWorker, cfg.tps, cfg.ErrorRate(), cfg.timing, cfg.Score, cfg.NumSelected)
}

func (cfg *consumerConfig) Equal(other *consumerConfig) bool {
	if other == nil {
		return false
	}
	return cfg.NumWorker == other.NumWorker && cfg.NumFetcher == other.NumFetcher
}

func (cfg *consumerConfig) Clone() *consumerConfig {
	return &consumerConfig{
		NumWorker:  cfg.NumWorker,
		NumFetcher: cfg.NumFetcher,
	}
}

//------------------------------------------------------------------------------

type configRoulette struct {
	opt *QueueOptions

	rnd     *rand.Rand
	cfgs    []*consumerConfig
	nextCfg uint

	maxTPS     float64
	maxTiming  time.Duration
	bestCfg    *consumerConfig
	oldBestCfg *consumerConfig
}

func newConfigRoulette(opt *QueueOptions) *configRoulette {
	r := &configRoulette{
		opt: opt,

		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	cfg := newConsumerConfig(1, int32(opt.MinNumWorker))
	r.resetConfigs(cfg, false)

	return r
}

func (r *configRoulette) Select(currCfg *consumerConfig, queueEmpty bool) *consumerConfig {
	if currCfg != nil {
		r.updateScores(currCfg)
		r.checkScores(queueEmpty)
	}

	var newCfg *consumerConfig
	if r.bestCfg == nil || r.rnd.Float64() <= 0.15 {
		idx := r.nextCfg % uint(len(r.cfgs))
		newCfg = r.cfgs[idx]
		r.nextCfg++
	} else {
		newCfg = r.bestCfg
	}

	newCfg.NumSelected++
	return newCfg
}

func (r *configRoulette) updateScores(cfg *consumerConfig) {
	var dirty bool
	if tps := cfg.TPS(); tps > r.maxTPS {
		r.maxTPS = tps
		dirty = true
	}
	if timing := cfg.Timing(); timing > r.maxTiming {
		r.maxTiming = timing
		dirty = true
	}

	cfg.SetScore(r.configScore(cfg))

	r.bestCfg = nil
	for _, c := range r.cfgs {
		if c.NumSelected == 0 {
			r.bestCfg = nil
			break
		}

		if dirty && c != cfg {
			c.SetScore(r.configScore(c))
		}

		if r.bestCfg == nil || c.Score-r.bestCfg.Score >= 0.01 {
			r.bestCfg = c
		}
	}
}

func (r *configRoulette) configScore(cfg *consumerConfig) float64 {
	tps := cfg.TPS()
	if tps == 0 {
		return 0
	}

	var score float64

	errorRate := 10 * cfg.ErrorRate()
	if errorRate > 1 {
		errorRate = 1
	}

	score += 0.35 * (1 - errorRate)

	if r.maxTPS != 0 {
		score += 0.35 * (tps / r.maxTPS)
	}

	if r.maxTiming != 0 {
		score += 0.3 * (1 - float64(cfg.Timing())/float64(r.maxTiming))
	}

	return score
}

func (r *configRoulette) checkScores(queueEmpty bool) {
	if r.bestCfg == nil || r.bestCfg.Equal(r.oldBestCfg) {
		return
	}
	if r.bestCfg.NumSelected < numSelectedThreshold {
		return
	}
	if queueEmpty && r.bestCfg.NumWorker > r.oldBestCfg.NumWorker {
		return
	}

	if false {
		for _, cfg := range r.cfgs {
			internal.Logger.Println("taskq: " + cfg.String())
		}
	}
	r.resetConfigs(r.bestCfg, queueEmpty)
}

func (r *configRoulette) resetConfigs(bestCfg *consumerConfig, queueEmpty bool) {
	r.genConfigs(bestCfg, queueEmpty)
	r.maxTPS = 0
	r.maxTiming = 0
	r.bestCfg = nil
}

func (r *configRoulette) genConfigs(bestCfg *consumerConfig, queueEmpty bool) {
	r.cfgs = make([]*consumerConfig, 0, 5)

	if bestCfg.NumFetcher > 1 {
		r.addConfig(r.withLessFetchers(bestCfg))
	}

	if bestCfg.NumWorker > r.opt.MinNumWorker {
		r.addConfig(r.withLessWorkers(bestCfg, bestCfg.NumWorker/4))
	}

	r.oldBestCfg = bestCfg.Clone()
	r.addConfig(r.oldBestCfg)

	if !hasFreeSystemResources(r.opt.MinSystemResources) {
		internal.Logger.Println("taskq: system does not have enough free resources")
		return
	}

	if queueEmpty {
		r.addConfig(r.withMoreWorkers(bestCfg, 2))
		return
	}

	if bestCfg.NumWorker < r.opt.MaxNumWorker {
		if r.oldBestCfg == nil || significant(bestCfg.Score-r.oldBestCfg.Score) {
			r.addConfig(r.withMoreWorkers(bestCfg, r.opt.MaxNumWorker/4))
		} else {
			r.addConfig(r.withMoreWorkers(bestCfg, 2))
		}
	}

	if bestCfg.NumFetcher < r.opt.MaxNumFetcher {
		r.addConfig(r.withMoreFetchers(bestCfg))
	}
}

func (r *configRoulette) addConfig(cfg *consumerConfig) {
	r.cfgs = append(r.cfgs, cfg)
}

func (r *configRoulette) withLessWorkers(cfg *consumerConfig, n int32) *consumerConfig {
	if n <= 0 {
		n = 1
	} else if n > 10 {
		n = 10
	}
	cfg = cfg.Clone()
	cfg.NumWorker -= n
	if cfg.NumWorker < r.opt.MinNumWorker {
		cfg.NumWorker = r.opt.MinNumWorker
	}
	return cfg
}

func (r *configRoulette) withMoreWorkers(cfg *consumerConfig, n int32) *consumerConfig {
	if n <= 0 {
		n = 1
	} else if n > 10 {
		n = 10
	}
	cfg = cfg.Clone()
	cfg.NumWorker += n
	if cfg.NumWorker > r.opt.MaxNumWorker {
		cfg.NumWorker = r.opt.MaxNumWorker
	}
	return cfg
}

func (r *configRoulette) withLessFetchers(cfg *consumerConfig) *consumerConfig {
	cfg = cfg.Clone()
	cfg.NumFetcher--
	return cfg
}

func (r *configRoulette) withMoreFetchers(cfg *consumerConfig) *consumerConfig {
	cfg = cfg.Clone()
	cfg.NumFetcher++
	return cfg
}

func significant(n float64) bool {
	return n > 0.05
}
