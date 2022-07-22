package main

import (
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gonum/stat"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	tenantState, err := loadTenantData()
	if err != nil {
		level.Error(logger).Log("err", err, "msg", "failed to load tenant data")
		os.Exit(1)
	}

	analyses := []AnalysisResult{}

	preEvictionAnalysis := analyzeSharding(tenantState)
	analyses = append(analyses, preEvictionAnalysis)
	fmt.Printf("preEvictionAnalysis: \n%v\n", preEvictionAnalysis)

	type job struct {
		maxSeries int
		work      func(TenantState, int) ([]BalanceJob, error)
	}

	var jobs = []job{}
	// for i := 0; i < 100; i++ {
	// 	jobs = append(jobs, job{
	// 		work:      evictLargestTenantOnOverloadedIngesters,
	// 		maxSeries: int(preEvictionAnalysis.IngesterSizes.Sum / 100),
	// 	})
	// }
	for i := 0; i < 1000; i++ {
		jobs = append(jobs, job{
			work:      evictTenantsOnAboveAverageIngesters,
			maxSeries: int(preEvictionAnalysis.IngesterSizes.Sum / 20),
		})
		jobs = append(jobs, job{
			work:      growShardSizes,
			maxSeries: int(preEvictionAnalysis.IngesterSizes.Sum / float64(i+1)),
		})
	}
	// for i := 0; i < 10; i++ {
	// 	jobs = append(jobs, job{
	// 		work:      evictLargestTenantOnOverloadedIngesters,
	// 		maxSeries: 1,
	// 	})
	// }

	for _, job := range jobs {

		// Evict up to 1% of data at once
		evictionJobs, err := job.work(tenantState, job.maxSeries)
		if err != nil {
			level.Error(logger).Log("err", err, "msg", "failed to plan evictions")
			os.Exit(1)
		}

		evictionJobs = filterEvictionJobs(evictionJobs)

		if len(evictionJobs) == 0 {
			level.Info(logger).Log("msg", "no evictions to run")
			continue
		}

		level.Info(logger).Log("msg", "evicting", "num_evictions", len(evictionJobs))

		for _, job := range evictionJobs {
			err = job.Apply(tenantState)
			if err != nil {
				level.Error(logger).Log("err", err, "msg", "failed to run eviction")
				os.Exit(1)
			}
		}

		reshard(tenantState)

		postEvictionAnalysis := analyzeSharding(tenantState)
		analyses = append(analyses, postEvictionAnalysis)

		fmt.Printf("postEvictionAnalysis: \n%v\n", postEvictionAnalysis)
	}

	longTermAnalyze(analyses)
	histPlot("final-ingester-sizes", analyses[len(analyses)-1].IngesterSizes.RawData)
	histPlot("initial-ingester-sizes", analyses[0].IngesterSizes.RawData)

	level.Info(logger).Log("msg", "eviction complete")
}

type TenantState struct {
	Ingesters []*Ingester
	Tenants   []*Tenant
}

type Ingester struct {
	ID        string
	NumSeries int
	Tenants   []string
}

type Tenant struct {
	ID        string
	Seed      int64
	NumSeries int
	ShardSize int
	Ingesters []string
}

type BalanceJob interface {
	Apply(TenantState) error
}

type EvictionJob struct {
	TenantID string
}

func (e EvictionJob) Apply(state TenantState) error {
	for _, t := range state.Tenants {
		if t.ID == e.TenantID {
			t.Seed++
		}
	}
	return nil
}

type IncreaseShardSizeJob struct {
	TenantID string
}

func (i IncreaseShardSizeJob) Apply(state TenantState) error {
	for _, t := range state.Tenants {
		if t.ID == i.TenantID {
			t.ShardSize = int(math.Max(1, math.Ceil(float64(t.NumSeries)/2e6)))
		}
	}
	return nil
}

func evictLargestTenantOnOverloadedIngesters(state TenantState, maxSeries int) ([]BalanceJob, error) {
	var tenantMap = make(map[string]*Tenant)
	for _, t := range state.Tenants {
		tenantMap[t.ID] = t
	}

	var seenIngesters = make(map[string]bool)
	var seenTenants = make(map[string]bool)

	var totalEvictions = 0
	var evictionJobs []BalanceJob

	for totalEvictions < maxSeries {
		var largestIngester *Ingester
		for _, i := range state.Ingesters {
			if seenIngesters[i.ID] {
				continue
			}

			if largestIngester == nil || i.NumSeries > largestIngester.NumSeries {
				largestIngester = i
			}
		}
		if largestIngester == nil {
			// If we run out of ingesters, that means we've asked all of them to evict at least one tenant, so we can stop.
			break
		}

		var largestTenant *Tenant
		for _, tenantID := range largestIngester.Tenants {
			if seenTenants[tenantID] {
				continue
			}

			tenant := tenantMap[tenantID]
			// Don't try to reshard ingesters already on all ingesters
			if tenant.ShardSize >= len(state.Ingesters) {
				continue
			}

			if largestTenant == nil || largestTenant.NumSeries < tenant.NumSeries {
				largestTenant = tenant
			}
		}

		if largestTenant == nil {
			// If we failed to find a tenant to evict, we're done
			break
		}

		seenTenants[largestTenant.ID] = true
		seenIngesters[largestIngester.ID] = true

		totalEvictions += largestTenant.NumSeries
		evictionJobs = append(evictionJobs, EvictionJob{TenantID: largestTenant.ID})
	}

	return evictionJobs, nil
}

func evictTenantsOnAboveAverageIngesters(state TenantState, maxSeries int) ([]BalanceJob, error) {
	var tenantMap = make(map[string]*Tenant)
	for _, t := range state.Tenants {
		tenantMap[t.ID] = t
	}
	var ingesterMap = make(map[string]*Ingester)
	for _, t := range state.Ingesters {
		ingesterMap[t.ID] = t
	}

	ingesterSizes := make([]float64, len(state.Ingesters))
	for i, ingester := range state.Ingesters {
		ingesterSizes[i] = float64(ingester.NumSeries)
	}

	sort.Float64s(ingesterSizes)
	median := stat.Quantile(0.95, stat.Empirical, ingesterSizes, nil)
	var aboveAverageMap = make(map[string]bool)
	for _, ingester := range state.Ingesters {
		if float64(ingester.NumSeries) > median {
			aboveAverageMap[ingester.ID] = true
		}
	}

	type TenantScore struct {
		Tenant string
		Score  float64
	}
	scores := []TenantScore{}
	for _, tenant := range state.Tenants {
		if tenant.ShardSize >= len(state.Ingesters) {
			continue
		}
		score := TenantScore{
			Tenant: tenant.ID,
		}
		for _, ingesterID := range tenant.Ingesters {
			if aboveAverageMap[ingesterID] {
				score.Score++
			} else {
				score.Score--
			}
		}
		// score.Score *= float64(tenant.NumSeries)
		// score.Score /= float64(tenant.ShardSize)
		scores = append(scores, score)
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	var totalEvictions int
	var jobs []BalanceJob
	seenIngesters := make(map[string]bool)
outer:
	for i := 0; totalEvictions < maxSeries && i < len(scores); i++ {
		if scores[i].Score <= 0 {
			break
		}
		totalEvictions += tenantMap[scores[i].Tenant].NumSeries

		for _, ingesterID := range tenantMap[scores[i].Tenant].Ingesters {
			if seenIngesters[ingesterID] {
				continue outer
			}
			seenIngesters[ingesterID] = true
		}

		jobs = append(jobs, EvictionJob{TenantID: scores[i].Tenant})
	}

	return jobs, nil
}

// This doesn't work at all
func evictTenantsCorrelatedWithIngesterSize(state TenantState, maxSeries int) ([]BalanceJob, error) {
	var tenantMap = make(map[string]*Tenant)
	for _, t := range state.Tenants {
		tenantMap[t.ID] = t
	}

	ingesterSizes := make([]float64, len(state.Ingesters))
	tenantContributions := make([][]float64, len(state.Tenants))
	for i, tenant := range state.Tenants {
		tenantContributions[i] = make([]float64, len(state.Ingesters))
		for j := range tenant.Ingesters {
			tenantContributions[i][j] = float64(tenant.NumSeries) / float64(len(tenant.Ingesters))
		}

	}

	for i, ingester := range state.Ingesters {
		ingesterSizes[i] = float64(ingester.NumSeries)
	}

	type TenantIngesterCorrelation struct {
		TenantID    string
		Correlation float64
		Series      float64
		Value       float64
	}
	var correlations []TenantIngesterCorrelation
	for i, tenant := range state.Tenants {
		if tenant.ShardSize >= len(state.Ingesters) {
			continue
		}

		corr := stat.Correlation(ingesterSizes, tenantContributions[i], nil)

		correlations = append(correlations, TenantIngesterCorrelation{
			TenantID:    tenant.ID,
			Correlation: corr,
			Series:      float64(tenant.NumSeries),
			Value:       corr * float64(tenant.NumSeries),
		})
	}

	sort.Slice(correlations, func(i, j int) bool {
		return correlations[i].Correlation > correlations[j].Correlation
	})

	var totalEvictions int
	var jobs []BalanceJob
	for i := 0; totalEvictions < maxSeries && i < len(correlations); i++ {
		totalEvictions += tenantMap[correlations[i].TenantID].NumSeries
		jobs = append(jobs, EvictionJob{TenantID: correlations[i].TenantID})
	}

	return jobs, nil
}

func growShardSizes(state TenantState, maxSeries int) ([]BalanceJob, error) {
	var output []BalanceJob
	for _, tenant := range state.Tenants {
		if tenant.ShardSize*2e6 < tenant.NumSeries {
			output = append(output, IncreaseShardSizeJob{TenantID: tenant.ID})
		}
	}
	return output, nil
}

func filterEvictionJobs(jobs []BalanceJob) []BalanceJob {
	return jobs
}
