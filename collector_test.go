package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestDecodeJSON(t *testing.T) {
	jsonFile, err := os.Open("example_metrics.json")
	if err != nil {
		t.Error(err)
	}

	var c clusterMetrics
	err = json.NewDecoder(jsonFile).Decode(&c)
	if err != nil {
		t.Error(err)
	}

	testValue(t, "AppsSubmitted", 1, c.ClusterMetrics.AppsSubmitted)
	testValue(t, "AppsCompleted", 2, c.ClusterMetrics.AppsCompleted)
	testValue(t, "AppsPending", 3, c.ClusterMetrics.AppsPending)
	testValue(t, "AppsRunning", 4, c.ClusterMetrics.AppsRunning)
	testValue(t, "AppsFailed", 5, c.ClusterMetrics.AppsFailed)
	testValue(t, "AppsKilled", 6, c.ClusterMetrics.AppsKilled)
	testValue(t, "ReservedMB", 7, c.ClusterMetrics.ReservedMB)
	testValue(t, "AvailableMB", 8, c.ClusterMetrics.AvailableMB)
	testValue(t, "AllocatedMB", 9, c.ClusterMetrics.AllocatedMB)
	testValue(t, "ReservedVirtualCores", 10, c.ClusterMetrics.ReservedVirtualCores)
	testValue(t, "AvailableVirtualCores", 11, c.ClusterMetrics.AvailableVirtualCores)
	testValue(t, "AllocatedVirtualCores", 12, c.ClusterMetrics.AllocatedVirtualCores)
	testValue(t, "ContainersAllocated", 13, c.ClusterMetrics.ContainersAllocated)
	testValue(t, "ContainersReserved", 14, c.ClusterMetrics.ContainersReserved)
	testValue(t, "ContainersPending", 15, c.ClusterMetrics.ContainersPending)
	testValue(t, "TotalMB", 16, c.ClusterMetrics.TotalMB)
	testValue(t, "TotalVirtualCores", 17, c.ClusterMetrics.TotalVirtualCores)
	testValue(t, "TotalNodes", 18, c.ClusterMetrics.TotalNodes)
	testValue(t, "LostNodes", 19, c.ClusterMetrics.LostNodes)
	testValue(t, "UnhealthyNodes", 20, c.ClusterMetrics.UnhealthyNodes)
	testValue(t, "DecommissioningNodes", 21, c.ClusterMetrics.DecommissioningNodes)
	testValue(t, "DecommissionedNodes", 22, c.ClusterMetrics.DecommissionedNodes)
	testValue(t, "RebootedNodes", 23, c.ClusterMetrics.RebootedNodes)
	testValue(t, "ActiveNodes", 24, c.ClusterMetrics.ActiveNodes)
	testValue(t, "ShutdownNodes", 25, c.ClusterMetrics.ShutdownNodes)
}

func testValue(t *testing.T, metric string, expected int, actual int) {
	if expected != actual {
		t.Errorf("error asserting [%s]: expected: %d, actual: %d", metric, expected, actual)
	}
}