package main

import (
	"fmt"
	"math"
	"math/rand"
)

func loadTenantData() (TenantState, error) {
	r := rand.New(rand.NewSource(0))

	var ingesters = make([]*Ingester, 500) // TODO: vary the number of ingesters
	for i := range ingesters {
		ingesters[i] = &Ingester{
			ID: fmt.Sprintf("ingester-%d", i),
		}
	}

	var tenants = make([]*Tenant, 5000) // TODO: vary the number of tenants
	for i := range tenants {
		numSeries := int(math.Abs(r.NormFloat64()*r.NormFloat64()*2e6 + 1))
		tenants[i] = &Tenant{
			ID:        fmt.Sprintf("tenant-%d", i),
			NumSeries: numSeries,
			ShardSize: int(math.Max(1, math.Ceil(float64(numSeries)/2e6))),
		}
	}

	output := TenantState{
		Ingesters: ingesters,
		Tenants:   tenants,
	}
	reshard(output)
	return output, nil
}
