package main

import (
	"github.com/duro-d/duro/routing/internal/controller"
	"github.com/prometheus/client_golang/prometheus"
)

// Only publish fresh observations. One gauge pair per quantity bounds cardinality
// across worker restarts and leaves a gap when the controller has no valid sample.
type loadMetrics struct {
	controller                           *controller.Controller
	queueMin, queueMax, busyMin, busyMax *prometheus.Desc
}

func newLoadMetrics(c *controller.Controller) *loadMetrics {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("duro_routing_worker_"+name, help, nil, nil)
	}
	return &loadMetrics{c, desc("queue_min", "Smallest observed worker queue."), desc("queue_max", "Largest observed worker queue."),
		desc("busy_ratio_min", "Lowest worker slot utilization."), desc("busy_ratio_max", "Highest worker slot utilization.")}
}

func (m *loadMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.queueMin, m.queueMax, m.busyMin, m.busyMax} {
		ch <- d
	}
}
func (m *loadMetrics) Collect(ch chan<- prometheus.Metric) {
	if !m.controller.IsLeader() {
		return
	}
	workers := m.controller.Metrics()
	if len(workers) == 0 {
		return
	}
	minQ, maxQ := workers[0].Queue, workers[0].Queue
	minB, maxB := workers[0].BusyFraction, workers[0].BusyFraction
	for _, reading := range workers[1:] {
		minQ, maxQ = min(minQ, reading.Queue), max(maxQ, reading.Queue)
		minB, maxB = min(minB, reading.BusyFraction), max(maxB, reading.BusyFraction)
	}
	ch <- prometheus.MustNewConstMetric(m.queueMin, prometheus.GaugeValue, float64(minQ))
	ch <- prometheus.MustNewConstMetric(m.queueMax, prometheus.GaugeValue, float64(maxQ))
	ch <- prometheus.MustNewConstMetric(m.busyMin, prometheus.GaugeValue, minB)
	ch <- prometheus.MustNewConstMetric(m.busyMax, prometheus.GaugeValue, maxB)
}
