package main

import (
	"math"
	"math/rand"

	"github.com/grafana/dskit/ring/shard"
)

func reshard(state TenantState) {
	// Reset ingester state
	for _, i := range state.Ingesters {
		i.NumSeries = 0
		i.Tenants = []string{}
	}

	for _, t := range state.Tenants {
		t.Ingesters = []string{}

		seed := shard.ShuffleShardSeed(t.ID, "")
		subRing := rand.New(rand.NewSource(seed + t.Seed))

		t.NumSeries += int(subRing.NormFloat64()*1000 + 500)
		if t.NumSeries < 0 {
			t.NumSeries = 0
		}

		seen := make(map[string]bool)
		for len(t.Ingesters) < t.ShardSize && len(t.Ingesters) < len(state.Ingesters) {
			ingester := state.Ingesters[subRing.Intn(len(state.Ingesters))]
			if seen[ingester.ID] {
				continue
			}

			seen[ingester.ID] = true
			t.Ingesters = append(t.Ingesters, ingester.ID)
			ingester.Tenants = append(ingester.Tenants, t.ID)
			ingester.NumSeries += t.NumSeries / int(math.Min(float64(len(state.Ingesters)), float64(t.ShardSize)))
		}
	}
}
