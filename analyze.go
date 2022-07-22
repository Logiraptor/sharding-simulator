package main

import (
	"fmt"
	"sort"

	"github.com/gonum/floats"
	"github.com/gonum/stat"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func longTermAnalyze(analyses []AnalysisResult) {
	// var ingesterSizeMeans = make([]float64, 0, len(analyses))
	// var ingesterSizeStddevs = make([]float64, 0, len(analyses))

	// for _, a := range analyses {
	// 	ingesterSizeMeans = append(ingesterSizeMeans, a.IngesterSizes.Mean)
	// 	ingesterSizeStddevs = append(ingesterSizeStddevs, a.IngesterSizes.StdDev)
	// }

	// linePlot("long-term-ingester-size-means", ingesterSizeMeans)
	// linePlot("long-term-ingester-size-stddevs", ingesterSizeStddevs)

	// Pick 5 evenly distributed analysis results to plot

	numPlots := 10
	plottedAnalyses := make([]AnalysisResult, 0, numPlots)
	divisor := len(analyses) / (numPlots - 1)

	for i := range analyses {
		if i%divisor == 0 || i == len(analyses)-1 {
			plottedAnalyses = append(plottedAnalyses, analyses[i])
		}
	}

	var ingesterSizes = make([]plotter.Values, 0, len(plottedAnalyses))
	var ingesterTenantCounts = make([]plotter.Values, 0, len(plottedAnalyses))
	for _, a := range plottedAnalyses {
		ingesterSizes = append(ingesterSizes, a.IngesterSizes.RawData)
		ingesterTenantCounts = append(ingesterTenantCounts, a.IngesterTenantCounts.RawData)
	}

	boxPlot("ingester-sizes", ingesterSizes)
	boxPlot("ingester-tenant-counts", ingesterTenantCounts)
}

type AnalysisResult struct {
	TenantSizes          SummaryStats
	TenantShardSizes     SummaryStats
	IngesterSizes        SummaryStats
	IngesterTenantCounts SummaryStats
}

func (a AnalysisResult) String() string {
	return fmt.Sprintf(`
TenantSizes:          %s
TenantShardSizes:     %s
IngesterSizes:        %s
IngesterTenantCounts: %s
`, a.TenantSizes, a.TenantShardSizes, a.IngesterSizes, a.IngesterTenantCounts)
}

type SummaryStats struct {
	N        int
	Min, Max float64
	Mean     float64
	Median   float64
	StdDev   float64
	Sum      float64
	RawData  []float64
}

func (s SummaryStats) String() string {
	return fmt.Sprintf("n: %d, min: %.2e, max: %.2e, mean: %.2e, median: %.2e, stddev: %.2e, sum: %.2e", s.N, s.Min, s.Max, s.Mean, s.Median, s.StdDev, s.Sum)
}

func summarize(name string, data []float64) SummaryStats {
	sort.Float64s(data)
	min := floats.Min(data)
	max := floats.Max(data)
	mean, stddev := stat.MeanStdDev(data, nil)
	median := stat.Quantile(0.5, stat.Empirical, data, nil)
	sum := floats.Sum(data)

	// histPlot(name, data)

	return SummaryStats{
		N:       len(data),
		Min:     min,
		Max:     max,
		Median:  median,
		Mean:    mean,
		StdDev:  stddev,
		Sum:     sum,
		RawData: data,
	}
}

func analyzeSharding(state TenantState) AnalysisResult {
	tenantSizes := make([]float64, 0, len(state.Tenants))
	tenantShardSizes := make([]float64, 0, len(state.Tenants))
	ingesterSizes := make([]float64, 0, len(state.Ingesters))
	ingesterTenantCounts := make([]float64, 0, len(state.Ingesters))

	for _, tenant := range state.Tenants {
		tenantSizes = append(tenantSizes, float64(tenant.NumSeries))
		tenantShardSizes = append(tenantShardSizes, float64(tenant.NumSeries)/float64(len(tenant.Ingesters)))
	}
	for _, ingester := range state.Ingesters {
		ingesterSizes = append(ingesterSizes, float64(ingester.NumSeries))
		ingesterTenantCounts = append(ingesterTenantCounts, float64(len(ingester.Tenants)))
	}

	return AnalysisResult{
		TenantSizes:          summarize("tenant-sizes", tenantSizes),
		TenantShardSizes:     summarize("tenant-shard-sizes", tenantShardSizes),
		IngesterSizes:        summarize("ingester-sizes", ingesterSizes),
		IngesterTenantCounts: summarize("ingester-tenant-counts", ingesterTenantCounts),
	}
}

func histPlot(name string, values plotter.Values) {
	p := plot.New()
	p.Title.Text = name

	hist, err := plotter.NewHist(values, 20)
	if err != nil {
		panic(err)
	}
	p.Add(hist)

	if err := p.Save(3*vg.Inch, 3*vg.Inch, fmt.Sprintf("%s-histogram.png", name)); err != nil {
		panic(err)
	}
}

func linePlot(name string, values plotter.Values) {
	p := plot.New()
	p.Title.Text = name

	points := make(plotter.XYs, len(values))
	for i, v := range values {
		points[i].X = float64(i)
		points[i].Y = v
	}

	hist, err := plotter.NewLine(points)
	if err != nil {
		panic(err)
	}
	p.Add(hist)

	if err := p.Save(3*vg.Inch, 3*vg.Inch, fmt.Sprintf("%s-line.png", name)); err != nil {
		panic(err)
	}
}

func boxPlot(name string, values []plotter.Values) {
	p := plot.New()
	p.Title.Text = name

	for i, record := range values {
		hist, err := plotter.NewBoxPlot(vg.Points(20), float64(i), record)
		if err != nil {
			panic(err)
		}
		p.Add(hist)
	}

	if err := p.Save(3*vg.Inch, vg.Inch*vg.Length(len(values)/2), fmt.Sprintf("%s-box-plot.png", name)); err != nil {
		panic(err)
	}
}
