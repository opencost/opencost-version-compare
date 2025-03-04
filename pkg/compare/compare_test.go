package compare

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/util"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/modules/prometheus-source/pkg/prom"

	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/errors"
	"github.com/opencost/opencost/pkg/metrics"
	prometheus "github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/opencost/opencost-compare/pkg/model"
)

var envVars map[string]string = map[string]string{
	"READ_ONLY":                                 "false",
	"PROMETHEUS_SERVER_ENDPOINT":                "http://localhost:9011",
	"CLOUD_PROVIDER_API_KEY":                    "AIzaSyDXQPG_MHUEy9neR7stolq6l0ujXmjJlvk",
	"CLUSTER_PROFILE":                           "production",
	"EMIT_POD_ANNOTATIONS_METRIC":               "false",
	"EMIT_NAMESPACE_ANNOTATIONS_METRIC":         "false",
	"EMIT_KSM_V1_METRICS":                       "true",
	"EMIT_KSM_V1_METRICS_ONLY":                  "false",
	"LOG_COLLECTION_ENABLED":                    "true",
	"PRODUCT_ANALYTICS_ENABLED":                 "true",
	"ERROR_REPORTING_ENABLED":                   "true",
	"VALUES_REPORTING_ENABLED":                  "true",
	"SENTRY_DSN":                                "https://71964476292e4087af8d5072afe43abd@o394722.ingest.sentry.io/5245431",
	"LEGACY_EXTERNAL_API_DISABLED":              "false",
	"CACHE_WARMING_ENABLED":                     "false",
	"SAVINGS_ENABLED":                           "true",
	"ETL_RESOLUTION_SECONDS":                    "300",
	"ETL_MAX_PROMETHEUS_QUERY_DURATION_MINUTES": "1440",
	"ETL_DAILY_STORE_DURATION_DAYS":             "91",
	"ETL_HOURLY_STORE_DURATION_HOURS":           "49",
	"ETL_WEEKLY_STORE_DURATION_WEEKS":           "53",
	"ETL_FILE_STORE_ENABLED":                    "true",
	"ETL_ASSET_RECONCILIATION_ENABLED":          "true",
	"CONTAINER_STATS_ENABLED":                   "true",
	"RECONCILE_NETWORK":                         "true",
	"KUBECOST_METRICS_POD_ENABLED":              "false",
	"PV_ENABLED":                                "true",
	"MAX_QUERY_CONCURRENCY":                     "5",
	"UTC_OFFSET":                                "+00:00",
	"CLUSTER_ID":                                "cluster-one",
	"COST_EVENTS_AUDIT_ENABLED":                 "false",
	"RELEASE_NAME":                              "opencost-compare",
	"KUBECOST_NAMESPACE":                        "kubecost",
	"KUBECOST_TOKEN":                            "not-applied",
	"DIAGNOSTICS_RUN_IN_COST_MODEL":             "false",
	"ASSET_INCLUDE_LOCAL_DISK_COST":             "true",
}

type waiter chan struct{}

func (w *waiter) wait() {
	<-*w
}

func (w *waiter) signal() {
	close(*w)
}

func setupEnv(t *testing.T) {
	t.Helper()

	for key, value := range envVars {
		t.Setenv(key, value)
	}

	tempDir := t.TempDir()
	configsPath := path.Join(tempDir, "var", "configs")
	os.MkdirAll(configsPath, 0755)

	dbPath := path.Join(tempDir, "var", "db")
	os.MkdirAll(dbPath, 0755)

	t.Setenv("CONFIG_PATH", configsPath)
	t.Setenv("DB_PATH", dbPath)
	//"CONFIG_PATH":                               "/var/configs/",
	//"DB_PATH":                                   "/var/db/",
}

// ComputeAllocationHandler returns the assets from the CostModel.
func newComputeAssetsHandler(a *costmodel.Accesses) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		qp := httputil.NewQueryParams(r.URL.Query())

		// Window is a required field describing the window of time over which to
		// compute allocation data.
		window, err := opencost.ParseWindowWithOffset(qp.Get("window", ""), env.GetParsedUTCOffset())
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid 'window' parameter: %s", err), http.StatusBadRequest)
			return
		}

		filterString := qp.Get("filter", "")

		assetSet, err := a.ComputeAssetsFromCostmodel(window, filterString)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting assets: %s", err), http.StatusInternalServerError)
			return
		}

		w.Write(costmodel.WrapData(append([]*opencost.AssetSet{}, assetSet), nil))
	}
}

func bootstrap(t *testing.T) (*costmodel.Accesses, waiter) {
	t.Helper()

	setupEnv(t)

	router := httprouter.New()
	a := costmodel.Initialize(router)

	router.GET("/allocation", a.ComputeAllocationHandler)
	router.GET("/allocation/summary", a.ComputeAllocationHandlerSummary)
	router.GET("/assets", newComputeAssetsHandler(a))
	if env.IsCarbonEstimatesEnabled() {
		router.GET("/assets/carbon", a.ComputeAssetsCarbonHandler)
	}

	time.Sleep(5 * time.Second)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", router)
	rootMux.Handle("/metrics", promhttp.Handler())
	telemetryHandler := metrics.ResponseMetricMiddleware(rootMux)
	handler := cors.AllowAll().Handler(telemetryHandler)

	// used to control shutdown for the server
	w := make(waiter)

	srv := &http.Server{
		Addr:    fmt.Sprintf("localhost:%d", env.GetAPIPort()),
		Handler: errors.PanicHandlerMiddleware(handler),
	}

	go func() {
		err := srv.ListenAndServe()
		t.Logf("Server exited: %s", err)
	}()

	go func() {
		w.wait()
		srv.Shutdown(context.Background())
	}()

	return a, w
}

func runHourlyComparisonTest(t *testing.T, name string, hours int, compareFn func(*testing.T, string, string, time.Time, time.Time) bool) {
	t.Helper()

	_, w := bootstrap(t)
	defer w.signal()

	last := time.Now().UTC().Truncate(time.Hour)
	begin := last.Add(-time.Duration(hours) * time.Hour)

	const (
		TestHost   = "http://localhost:9003"
		TargetHost = "http://localhost:9007"
	)

	for current := begin; !current.Equal(last); current = current.Add(time.Hour) {
		start := current
		end := start.Add(time.Hour)

		if !compareFn(t, TestHost, TargetHost, start, end) {
			t.Fatalf("%s Comparison failed for %s to %s", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
		}

		t.Logf("%s are Equal from: %s to %s", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
}

func TestAllocationCompute(t *testing.T) {
	t.Logf("KUBECOST_NAMESPACE: %s", os.Getenv("KUBECOST_NAMESPACE"))
	//t.SkipNow()
	const backtrackHours = 5

	runHourlyComparisonTest(t, "Allocations", backtrackHours, CompareAllocations)
}

func TestAssetsCompute(t *testing.T) {
	//t.SkipNow()
	const backtrackHours = 3

	runHourlyComparisonTest(t, "Assets", backtrackHours, CompareAssets)
}

func TestCostModelData(t *testing.T) {
	// This test is flakey due to the way the /costDataModel endpoint is implemented
	// It uses an offset duration (relative to now() on the server) to fetch the data
	// This means that if the request takes seconds to arrive, the offset will create a
	// different start/end time for the data, leading to a mismatch.
	t.SkipNow()

	const backtrackHours = 24

	runHourlyComparisonTest(t, "CostData", backtrackHours, CompareCostModelData)
}

func TestClusterDirect(t *testing.T) {
	a, w := bootstrap(t)
	defer w.signal()

	dataSource := a.DataSource
	promDS := dataSource.(*prom.PrometheusDataSource)
	promCli := promDS.PrometheusClient()
	promConfig := promDS.PrometheusConfig()

	end := time.Now().Truncate(time.Hour)
	start := end.Add(-time.Hour)

	ok, deltas := ClusterDisksDirect(t, dataSource, promCli, promConfig, a.CloudProvider, start, end)
	if !ok {
		for _, delta := range deltas {
			t.Logf("[ClusterDisks] %s", delta)
		}
	}

	ok, deltas = ClusterLBsDirect(t, dataSource, promCli, promConfig, a.CloudProvider, start, end)
	if !ok {
		for _, delta := range deltas {
			t.Logf("[ClusterLBs] %s", delta)
		}
	}
}

func ClusterDisksDirect(t *testing.T, dataSource source.OpenCostDataSource, promCli prometheus.Client, promConfig *prom.OpenCostPrometheusConfig, cp models.Provider, start, end time.Time) (bool, []string) {
	t.Helper()

	deltas := []string{}

	legacyDisks, e := ClusterDisks(promCli, promConfig, cp, start, end)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	legacyDisksMap := make(map[string]*Disk)
	for k, v := range legacyDisks {
		key := k.Cluster + "," + k.Name
		legacyDisksMap[key] = v
	}

	currentDisks, e := costmodel.ClusterDisks(dataSource, cp, start, end)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	if len(legacyDisks) != len(currentDisks) {
		deltas = append(deltas, fmt.Sprintf("Length mismatch: %d != %d", len(legacyDisks), len(currentDisks)))
	}

	for k, disk := range currentDisks {
		key := k.Cluster + "," + k.Name
		t.Logf("Checking Disk Key: %s\n", key)

		ld, ok := legacyDisksMap[key]
		if !ok {
			deltas = append(deltas, fmt.Sprintf("Disk not found: %s", key))
			continue
		}

		if ld.Bytes != disk.Bytes {
			deltas = append(deltas, fmt.Sprintf("Bytes mismatch: %s: %f != %f", key, ld.Bytes, disk.Bytes))
		}

		if eq, msg := float64PtrCmp(ld.BytesUsedAvgPtr, disk.BytesUsedAvgPtr); !eq {
			deltas = append(deltas, fmt.Sprintf("BytesUsedAvgPtr mismatch: %s: %s", key, msg))
		}
		if eq, msg := float64PtrCmp(ld.BytesUsedMaxPtr, disk.BytesUsedMaxPtr); !eq {
			deltas = append(deltas, fmt.Sprintf("BytesUsedMaxPtr mismatch: %s: %s", key, msg))
		}

		if ld.ClaimName != disk.ClaimName {
			deltas = append(deltas, fmt.Sprintf("ClaimName mismatch: %s: %s != %s", key, ld.ClaimName, disk.ClaimName))
		}
		if ld.ClaimNamespace != disk.ClaimNamespace {
			deltas = append(deltas, fmt.Sprintf("ClaimNamespace mismatch: %s: %s != %s", key, ld.ClaimNamespace, disk.ClaimNamespace))
		}
		if ld.Cluster != disk.Cluster {
			deltas = append(deltas, fmt.Sprintf("Cluster mismatch: %s: %s != %s", key, ld.Cluster, disk.Cluster))
		}
		if ld.Name != disk.Name {
			deltas = append(deltas, fmt.Sprintf("Name mismatch: %s: %s != %s", key, ld.Name, disk.Name))
		}
		t.Logf("Costs: legacy: %f == current: %f", ld.Cost, disk.Cost)
		if !floatsEqual(ld.Cost, disk.Cost, 0.0001) {
			deltas = append(deltas, fmt.Sprintf("Cost mismatch: %s: %f != %f", key, ld.Cost, disk.Cost))
		}
		if ld.StorageClass != disk.StorageClass {
			deltas = append(deltas, fmt.Sprintf("StorageClass mismatch: %s: %s != %s", key, ld.StorageClass, disk.StorageClass))
		}
		if ld.VolumeName != disk.VolumeName {
			deltas = append(deltas, fmt.Sprintf("VolumeName mismatch: %s: %s != %s", key, ld.VolumeName, disk.VolumeName))
		}
	}

	return len(deltas) == 0, deltas
}

func ClusterLBsDirect(t *testing.T, dataSource source.OpenCostDataSource, promCli prometheus.Client, promConfig *prom.OpenCostPrometheusConfig, cp models.Provider, start, end time.Time) (bool, []string) {
	t.Helper()

	deltas := []string{}

	legacyLBs, e := ClusterLoadBalancers(promCli, promConfig, start, end)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	legacyLBsMap := make(map[string]*LoadBalancer)
	for k, v := range legacyLBs {
		key := k.Cluster + "," + k.Namespace + "," + k.Namespace + "/" + k.Name
		legacyLBsMap[key] = v
	}

	currentLBs, e := costmodel.ClusterLoadBalancers(dataSource, start, end)
	if e != nil {
		t.Fatalf("Error: %s", e)
	}

	if len(legacyLBs) != len(currentLBs) {
		deltas = append(deltas, fmt.Sprintf("Length mismatch: %d != %d", len(legacyLBs), len(currentLBs)))
	}

	for k, lb := range currentLBs {
		key := k.Cluster + "," + k.Namespace + "," + k.Name
		t.Logf("Checking LB Key: %s\n", key)

		llb, ok := legacyLBsMap[key]
		if !ok {
			deltas = append(deltas, fmt.Sprintf("LB not found: %s", key))
			continue
		}

		if llb.Cluster != lb.Cluster {
			deltas = append(deltas, fmt.Sprintf("Cluster mismatch: %s: %s != %s", key, llb.Cluster, lb.Cluster))
		}
		if llb.Namespace != lb.Namespace {
			deltas = append(deltas, fmt.Sprintf("Namespace mismatch: %s: %s != %s", key, llb.Namespace, lb.Namespace))
		}
		if llb.Name != lb.Name {
			deltas = append(deltas, fmt.Sprintf("Name mismatch: %s: %s != %s", key, llb.Name, lb.Name))
		}
		t.Logf("Costs: legacy: %f == current: %f", llb.Cost, lb.Cost)
		if !floatsEqual(llb.Cost, lb.Cost, 0.0001) {
			deltas = append(deltas, fmt.Sprintf("Cost mismatch: %s: %f != %f", key, llb.Cost, lb.Cost))
		}
		if llb.Ip != lb.Ip {
			deltas = append(deltas, fmt.Sprintf("Ip mismatch: %s: %s != %s", key, llb.Ip, lb.Ip))
		}
		if llb.Private != lb.Private {
			deltas = append(deltas, fmt.Sprintf("Private mismatch: %s: %t != %t", key, llb.Private, lb.Private))
		}
		if llb.ProviderID != lb.ProviderID {
			deltas = append(deltas, fmt.Sprintf("ProviderID mismatch: %s: %s != %s", key, llb.ProviderID, lb.ProviderID))
		}
	}

	return len(deltas) == 0, deltas
}

type GetEntitiesFunc[T any] = func(*testing.T, string, time.Time, time.Time) map[string]T
type CmpFunc[T any] = func(a, b T) (bool, []string)

func CompareAllocations(t *testing.T, aHost string, bHost string, start time.Time, end time.Time) bool {
	return CompareEntities(t, aHost, bHost, start, end, GetAllocations, AreAllocationResponseItemsEqual)
}

func CompareCostModelData(t *testing.T, aHost string, bHost string, start time.Time, end time.Time) bool {
	return CompareEntities(t, aHost, bHost, start, end, GetCostModelData, AreCostModelDataEqual)
}

func CompareAssets(t *testing.T, aHost string, bHost string, start time.Time, end time.Time) bool {
	return CompareEntities(t, aHost, bHost, start, end, GetAsset, AreAssetsResponseItemsEqual)
}

func CompareEntities[T any](
	t *testing.T,
	aHost string,
	bHost string,
	start time.Time,
	end time.Time,
	getFn GetEntitiesFunc[T],
	cmpFn CmpFunc[T],
) bool {
	t.Helper()

	return CompareEntityMaps(
		getFn(t, aHost, start, end),
		getFn(t, bHost, start, end),
		cmpFn,
	)
}

func CompareEntityMaps[T any](a, b map[string]T, cmpFn CmpFunc[T]) bool {
	success := true

	if len(a) != len(b) {
		keysInANotB := []string{}
		keysInBNotA := []string{}

		fmt.Printf("-- Length mismatch: %d != %d --\n", len(a), len(b))
		fmt.Printf("A Keys:\n")
		for key := range a {
			if _, ok := b[key]; !ok {
				keysInANotB = append(keysInANotB, key)
			}
			fmt.Printf("  %s\n", key)
		}
		fmt.Printf("\n\nB Keys:\n")
		for key := range b {
			if _, ok := a[key]; !ok {
				keysInBNotA = append(keysInBNotA, key)
			}
			fmt.Printf("  %s\n", key)
		}

		if len(keysInANotB) > 0 {
			fmt.Printf("\n\nKeys in A, Not B:\n")
			for _, key := range keysInANotB {
				fmt.Printf("  %s\n", key)
			}
		}

		if len(keysInBNotA) > 0 {
			fmt.Printf("\n\nKeys in B, Not A:\n")
			for _, key := range keysInBNotA {
				fmt.Printf("  %s\n", key)
			}
		}

		success = false
	}

	for key, item := range a {
		other, ok := b[key]
		if !ok {
			fmt.Printf("+----------------------------------------------------------------------------\n")
			fmt.Printf("| [%s] Deltas\n", key)
			fmt.Printf("+----------------------------------------------------------------------------\n")
			fmt.Printf("| * Could not find key: %s\n", key)
			fmt.Printf("+----------------------------------------------------------------------------\n")
			success = false
			continue
		}

		ok, deltas := cmpFn(item, other)
		if !ok {
			success = false
			fmt.Printf("+----------------------------------------------------------------------------\n")
			fmt.Printf("| [%s] Deltas\n", key)
			fmt.Printf("+----------------------------------------------------------------------------\n")
			for _, delta := range deltas {
				fmt.Printf("| * %s\n", delta)
			}
			fmt.Printf("+----------------------------------------------------------------------------\n")
		}
	}

	return success
}

// Equal compares two AllocationResponseItem instances for equality
func AreAllocationResponseItemsEqual(ari, other model.AllocationResponseItem) (bool, []string) {
	deltas := []string{}

	// Compare basic string field
	if ari.Name != other.Name {
		deltas = append(deltas, fmt.Sprintf("a.Name: %s != b.Name: %s", ari.Name, other.Name))
	}

	// Compare Properties pointer
	if (ari.Properties == nil) != (other.Properties == nil) {
		deltas = append(deltas, fmt.Sprintf("a.Properties == nil: %t, b.Properties == nil: %t", ari.Properties == nil, other.Properties == nil))
	}

	if ari.Properties != nil {
		if ari.Properties.Cluster != other.Properties.Cluster {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Cluster: %s, b.Properties.Cluster: %s", ari.Properties.Cluster, other.Properties.Cluster))
		}
		if ari.Properties.Node != other.Properties.Node {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Node: %s, b.Properties.Node: %s", ari.Properties.Node, other.Properties.Node))
		}
		if ari.Properties.Container != other.Properties.Container {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Container: %s, b.Properties.Container: %s", ari.Properties.Container, other.Properties.Container))
		}
		if ari.Properties.Controller != other.Properties.Controller {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Controller: %s, b.Properties.Controller: %s", ari.Properties.Controller, other.Properties.Controller))
		}
		if ari.Properties.ControllerKind != other.Properties.ControllerKind {
			deltas = append(deltas, fmt.Sprintf("a.Properties.ControllerKind: %s, b.Properties.ControllerKind: %s", ari.Properties.ControllerKind, other.Properties.ControllerKind))
		}
		if ari.Properties.Namespace != other.Properties.Namespace {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Namespace: %s, b.Properties.Namespace: %s", ari.Properties.Namespace, other.Properties.Namespace))
		}
		if ari.Properties.Pod != other.Properties.Pod {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Pod: %s, b.Properties.Pod: %s", ari.Properties.Pod, other.Properties.Pod))
		}
		if ari.Properties.ProviderID != other.Properties.ProviderID {
			deltas = append(deltas, fmt.Sprintf("a.Properties.ProviderID: %s, b.Properties.ProviderID: %s", ari.Properties.ProviderID, other.Properties.ProviderID))
		}

		// Compare slices and maps within Properties
		if !stringSliceEqual(ari.Properties.Services, other.Properties.Services) {
			deltas = append(deltas, "a.Properties.Services != b.Properties.Services")
		}
		if !stringMapEqual(ari.Properties.Labels, other.Properties.Labels) {
			//deltas = append(deltas, "a.Properties.Labels != b.Properties.Labels")
		}
		if !stringMapEqual(ari.Properties.Annotations, other.Properties.Annotations) {
			deltas = append(deltas, "a.Properties.Annotations != b.Properties.Annotations")
		}
		if !stringMapEqual(ari.Properties.NamespaceLabels, other.Properties.NamespaceLabels) {
			deltas = append(deltas, "a.Properties.NamespaceLabels != b.Properties.NamespaceLabels")
		}
		if !stringMapEqual(ari.Properties.NamespaceAnnotations, other.Properties.NamespaceAnnotations) {
			deltas = append(deltas, "a.Properties.NamespaceAnnotations != b.Properties.NamespaceAnnotations")
		}
	}

	// Compare Window
	if !ari.Window.Start.Equal(other.Window.Start) {
		deltas = append(deltas, fmt.Sprintf("a.Window.Start: %s != b.Window.Start: %s", ari.Window.Start, other.Window.Start))
	}
	if !ari.Window.End.Equal(other.Window.End) {
		deltas = append(deltas, fmt.Sprintf("a.Window.End: %s != b.Window.End: %s", ari.Window.End, other.Window.End))
	}

	// Compare time fields
	if !ari.Start.Equal(other.Start) {
		deltas = append(deltas, fmt.Sprintf("a.Start: %s != b.Start: %s", ari.Start, other.Start))
	}
	if !ari.End.Equal(other.End) {
		deltas = append(deltas, fmt.Sprintf("a.End: %s != b.End: %s", ari.End, other.End))
	}

	// Compare float64 fields with tolerance for floating point precision
	const epsilon = 0.00001
	if !floatsEqual(ari.CPUCoreHours, other.CPUCoreHours, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCoreHours: %f != b.CPUCoreHours: %f", ari.CPUCoreHours, other.CPUCoreHours))
	}
	if !floatsEqual(ari.CPUCoreRequestAverage, other.CPUCoreRequestAverage, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCoreRequestAverage: %f != b.CPUCoreRequestAverage: %f", ari.CPUCoreRequestAverage, other.CPUCoreRequestAverage))
	}
	if !floatsEqual(ari.CPUCoreUsageAverage, other.CPUCoreUsageAverage, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCoreUsageAverage: %f != b.CPUCoreUsageAverage: %f", ari.CPUCoreUsageAverage, other.CPUCoreUsageAverage))
	}
	if !floatsEqual(ari.CPUCost, other.CPUCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCost: %f != b.CPUCost: %f", ari.CPUCost, other.CPUCost))
	}
	if !floatsEqual(ari.CPUCostAdjustment, other.CPUCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCostAdjustment: %f != b.CPUCostAdjustment: %f", ari.CPUCostAdjustment, other.CPUCostAdjustment))
	}
	if !floatsEqual(ari.CPUCostIdle, other.CPUCostIdle, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.CPUCostIdle: %f != b.CPUCostIdle: %f", ari.CPUCostIdle, other.CPUCostIdle))
	}
	if !floatsEqual(ari.GPUHours, other.GPUHours, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.GPUHours: %f != b.GPUHours: %f", ari.GPUHours, other.GPUHours))
	}
	if !floatsEqual(ari.GPUCost, other.GPUCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.GPUCost: %f != b.GPUCost: %f", ari.GPUCost, other.GPUCost))
	}
	if !floatsEqual(ari.GPUCostAdjustment, other.GPUCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.GPUCostAdjustment: %f != b.GPUCostAdjustment: %f", ari.GPUCostAdjustment, other.GPUCostAdjustment))
	}
	if !floatsEqual(ari.GPUCostIdle, other.GPUCostIdle, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.GPUCostIdle: %f != b.GPUCostIdle: %f", ari.GPUCostIdle, other.GPUCostIdle))
	}
	if !floatsEqual(ari.NetworkTransferBytes, other.NetworkTransferBytes, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkTransferBytes: %f != b.NetworkTransferBytes: %f", ari.NetworkTransferBytes, other.NetworkTransferBytes))
	}
	if !floatsEqual(ari.NetworkReceiveBytes, other.NetworkReceiveBytes, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkReceiveBytes: %f != b.NetworkReceiveBytes: %f", ari.NetworkReceiveBytes, other.NetworkReceiveBytes))
	}
	if !floatsEqual(ari.NetworkCost, other.NetworkCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkCost: %f != b.NetworkCost: %f", ari.NetworkCost, other.NetworkCost))
	}
	if !floatsEqual(ari.NetworkCrossZoneCost, other.NetworkCrossZoneCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkCrossZoneCost: %f != b.NetworkCrossZoneCost: %f", ari.NetworkCrossZoneCost, other.NetworkCrossZoneCost))
	}
	if !floatsEqual(ari.NetworkCrossRegionCost, other.NetworkCrossRegionCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkCrossRegionCost: %f != b.NetworkCrossRegionCost: %f", ari.NetworkCrossRegionCost, other.NetworkCrossRegionCost))
	}
	if !floatsEqual(ari.NetworkInternetCost, other.NetworkInternetCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkInternetCost: %f != b.NetworkInternetCost: %f", ari.NetworkInternetCost, other.NetworkInternetCost))
	}
	if !floatsEqual(ari.NetworkCostAdjustment, other.NetworkCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.NetworkCostAdjustment: %f != b.NetworkCostAdjustment: %f", ari.NetworkCostAdjustment, other.NetworkCostAdjustment))
	}
	if !floatsEqual(ari.LoadBalancerCost, other.LoadBalancerCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.LoadBalancerCost: %f != b.LoadBalancerCost: %f", ari.LoadBalancerCost, other.LoadBalancerCost))
	}
	if !floatsEqual(ari.LoadBalancerCostAdjustment, other.LoadBalancerCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.LoadBalancerCostAdjustment: %f != b.LoadBalancerCostAdjustment: %f", ari.LoadBalancerCostAdjustment, other.LoadBalancerCostAdjustment))
	}
	if !floatsEqual(ari.PersistentVolumeCostAdjustment, other.PersistentVolumeCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.PersistentVolumeCostAdjustment: %f != b.PersistentVolumeCostAdjustment: %f", ari.PersistentVolumeCostAdjustment, other.PersistentVolumeCostAdjustment))
	}
	if !floatsEqual(ari.RAMByteHours, other.RAMByteHours, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMByteHours: %f != b.RAMByteHours: %f", ari.RAMByteHours, other.RAMByteHours))
	}
	if !floatsEqual(ari.RAMBytesRequestAverage, other.RAMBytesRequestAverage, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMBytesRequestAverage: %f != b.RAMBytesRequestAverage: %f", ari.RAMBytesRequestAverage, other.RAMBytesRequestAverage))
	}
	if !floatsEqual(ari.RAMBytesUsageAverage, other.RAMBytesUsageAverage, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMBytesUsageAverage: %f != b.RAMBytesUsageAverage: %f", ari.RAMBytesUsageAverage, other.RAMBytesUsageAverage))
	}
	if !floatsEqual(ari.RAMCost, other.RAMCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMCost: %f != b.RAMCost: %f", ari.RAMCost, other.RAMCost))
	}
	if !floatsEqual(ari.RAMCostAdjustment, other.RAMCostAdjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMCostAdjustment: %f != b.RAMCostAdjustment: %f", ari.RAMCostAdjustment, other.RAMCostAdjustment))
	}
	if !floatsEqual(ari.RAMCostIdle, other.RAMCostIdle, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.RAMCostIdle: %f != b.RAMCostIdle: %f", ari.RAMCostIdle, other.RAMCostIdle))
	}
	if !floatsEqual(ari.SharedCost, other.SharedCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.SharedCost: %f != b.SharedCost: %f", ari.SharedCost, other.SharedCost))
	}
	if !floatsEqual(ari.TotalCost, other.TotalCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.TotalCost: %f != b.TotalCost: %f", ari.TotalCost, other.TotalCost))
	}
	if !floatsEqual(ari.TotalEfficiency, other.TotalEfficiency, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.TotalEfficiency: %f != b.TotalEfficiency: %f", ari.TotalEfficiency, other.TotalEfficiency))
	}

	// Compare PersistentVolumes map
	if !persistentVolumesEqual(ari.PersistentVolumes, other.PersistentVolumes) {
		deltas = append(deltas, "a.PersistentVolumes != b.PersistentVolumes")
	}

	return len(deltas) == 0, deltas
}

// Helper functions for equality comparisons

// floatsEqual compares two float64 values with a given epsilon tolerance
func floatsEqual(a, b, epsilon float64) bool {
	if a == b {
		return true
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}

// stringSliceEqual compares two string slices for equality
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for _, s := range a {
		if !slices.Contains(b, s) {
			return false
		}
	}

	return true
}

// stringMapEqual compares two string maps for equality
func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// persistentVolumesEqual compares two AllocationResponseItemPersistentVolumes maps
func persistentVolumesEqual(a, b model.AllocationResponseItemPersistentVolumes) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}

	const epsilon = 0.00001
	for k, v := range a {
		otherPV, ok := b[k]
		if !ok {
			return false
		}

		if !floatsEqual(v.ByteHours, otherPV.ByteHours, epsilon) ||
			!floatsEqual(v.Cost, otherPV.Cost, epsilon) ||
			!floatsEqual(v.Adjustment, otherPV.Adjustment, epsilon) ||
			v.ProviderID != otherPV.ProviderID {
			return false
		}
	}

	return true
}

// AreAssetsResponseItemsEqual compares two AssetsResponseItem instances for equality
func AreAssetsResponseItemsEqual(ari, other model.AssetsResponseItem) (bool, []string) {
	deltas := []string{}

	// Compare Properties pointer
	if (ari.Properties == nil) != (other.Properties == nil) {
		deltas = append(deltas, fmt.Sprintf("a.Properties == nil: %t, b.Properties == nil: %t", ari.Properties == nil, other.Properties == nil))
	}

	if ari.Properties != nil {
		if ari.Properties.Category != other.Properties.Category {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Category: %s, b.Properties.Category: %s", ari.Properties.Category, other.Properties.Category))
		}
		if ari.Properties.Provider != other.Properties.Provider {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Provider: %s, b.Properties.Provider: %s", ari.Properties.Provider, other.Properties.Provider))
		}
		if ari.Properties.Account != other.Properties.Account {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Account: %s, b.Properties.Account: %s", ari.Properties.Account, other.Properties.Account))
		}
		if ari.Properties.Project != other.Properties.Project {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Project: %s, b.Properties.Project: %s", ari.Properties.Project, other.Properties.Project))
		}
		if ari.Properties.Service != other.Properties.Service {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Service: %s, b.Properties.Service: %s", ari.Properties.Service, other.Properties.Service))
		}
		if ari.Properties.Cluster != other.Properties.Cluster {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Cluster: %s, b.Properties.Cluster: %s", ari.Properties.Cluster, other.Properties.Cluster))
		}
		if ari.Properties.Name != other.Properties.Name {
			deltas = append(deltas, fmt.Sprintf("a.Properties.Name: %s, b.Properties.Name: %s", ari.Properties.Name, other.Properties.Name))
		}
		if ari.Properties.ProviderID != other.Properties.ProviderID {
			deltas = append(deltas, fmt.Sprintf("a.Properties.ProviderID: %s, b.Properties.ProviderID: %s", ari.Properties.ProviderID, other.Properties.ProviderID))
		}
	}

	// Compare Labels map
	if !stringMapEqual(ari.Labels, other.Labels) {
		deltas = append(deltas, "a.Labels != b.Labels")
	}

	// Compare Window
	if !ari.Window.Start.Equal(other.Window.Start) {
		deltas = append(deltas, fmt.Sprintf("a.Window.Start: %s != b.Window.Start: %s", ari.Window.Start, other.Window.Start))
	}
	if !ari.Window.End.Equal(other.Window.End) {
		deltas = append(deltas, fmt.Sprintf("a.Window.End: %s != b.Window.End: %s", ari.Window.End, other.Window.End))
	}

	// Compare time fields
	if !ari.Start.Equal(other.Start) {
		deltas = append(deltas, fmt.Sprintf("a.Start: %s != b.Start: %s", ari.Start, other.Start))
	}
	if !ari.End.Equal(other.End) {
		deltas = append(deltas, fmt.Sprintf("a.End: %s != b.End: %s", ari.End, other.End))
	}

	// Compare float64 fields with tolerance for floating point precision
	const epsilon = 0.00001
	if !floatsEqual(ari.Minutes, other.Minutes, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.Minutes: %f != b.Minutes: %f", ari.Minutes, other.Minutes))
	}
	if !floatsEqual(ari.Adjustment, other.Adjustment, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.Adjustment: %f != b.Adjustment: %f", ari.Adjustment, other.Adjustment))
	}
	if !floatsEqual(ari.TotalCost, other.TotalCost, epsilon) {
		deltas = append(deltas, fmt.Sprintf("a.TotalCost: %f != b.TotalCost: %f", ari.TotalCost, other.TotalCost))
	}

	return len(deltas) == 0, deltas
}

func AreCostModelDataEqual(a, b *costmodel.CostData) (bool, []string) {
	deltas := []string{}

	if a.Name != b.Name {
		deltas = append(deltas, fmt.Sprintf("a.Name: %s != b.Name: %s", a.Name, b.Name))
	}
	if a.PodName != b.PodName {
		deltas = append(deltas, fmt.Sprintf("a.PodName: %s != b.PodName: %s", a.PodName, b.PodName))
	}
	if a.NodeName != b.NodeName {
		deltas = append(deltas, fmt.Sprintf("a.NodeName: %s != b.NodeName: %s", a.NodeName, b.NodeName))
	}
	if a.Namespace != b.Namespace {
		deltas = append(deltas, fmt.Sprintf("a.Namespace: %s != b.Namespace: %s", a.Namespace, b.Namespace))
	}

	if !stringSliceEqual(a.Deployments, b.Deployments) {
		deltas = append(deltas, "a.Deployments != b.Deployments")
	}
	if !stringSliceEqual(a.Services, b.Services) {
		deltas = append(deltas, "a.Services != b.Services")
	}
	if !stringSliceEqual(a.Daemonsets, b.Daemonsets) {
		deltas = append(deltas, "a.Daemonsets != b.Daemonsets")
	}
	if !stringSliceEqual(a.Statefulsets, b.Statefulsets) {
		deltas = append(deltas, "a.Statefulsets != b.Statefulsets")
	}
	if !stringSliceEqual(a.Jobs, b.Jobs) {
		deltas = append(deltas, "a.Jobs != b.Jobs")
	}

	const (
		epsilon         = 0.00001
		ramAllocEpsilon = 20.0 * 1024.0 * 1024.0
	)

	if ok, msg := vectorCompare(a.RAMReq, b.RAMReq, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.RAMReq != b.RAMReq:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.RAMUsed, b.RAMUsed, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.RAMUsed != b.RAMUsed:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.RAMAllocation, b.RAMAllocation, ramAllocEpsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.RAMAllocation != b.RAMAllocation:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.CPUReq, b.CPUReq, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.CPUReq != b.CPUReq:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.CPUUsed, b.CPUUsed, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.CPUUsed != b.CPUUsed:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.CPUAllocation, b.CPUAllocation, epsilon /*0.0001*/); !ok {
		deltas = append(deltas, fmt.Sprintf("a.CPUAllocation != b.CPUAllocation:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.GPUReq, b.GPUReq, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.GPUReq != b.GPUReq:\n%s", msg))
	}
	if ok, msg := vectorCompare(a.NetworkData, b.NetworkData, epsilon); !ok {
		deltas = append(deltas, fmt.Sprintf("a.NetworkData != b.NetworkData:\n%s", msg))
	}

	if !stringMapEqual(a.Annotations, b.Annotations) {
		deltas = append(deltas, "a.Annotations != b.Annotations")
	}
	if !stringMapEqual(a.Labels, b.Labels) {
		deltas = append(deltas, "a.Labels != b.Labels")
	}
	if !stringMapEqual(a.NamespaceLabels, b.NamespaceLabels) {
		deltas = append(deltas, "a.NamespaceLabels != b.NamespaceLabels")
	}

	if a.ClusterID != b.ClusterID {
		deltas = append(deltas, fmt.Sprintf("a.ClusterID: %s != b.ClusterID: %s", a.ClusterID, b.ClusterID))
	}
	if a.ClusterName != b.ClusterName {
		deltas = append(deltas, fmt.Sprintf("a.ClusterName: %s != b.ClusterName: %s", a.ClusterName, b.ClusterName))
	}

	//NodeData
	//PVCData

	return len(deltas) == 0, deltas
}

func float64PtrCmp(a, b *float64) (bool, string) {
	if a == nil && b == nil {
		return true, ""
	}

	if a == nil || b == nil {
		return false, fmt.Sprintf("a: %v, b: %v", a, b)
	}

	if !floatsEqual(*a, *b, 0.0001) {
		return false, fmt.Sprintf("a: %v, b: %v", *a, *b)
	}

	return true, ""
}

func vectorCompare(a, b []*util.Vector, valueEpsilon float64) (bool, string) {
	if len(a) != len(b) {
		return false, fmt.Sprintf("len(a): %d != len(b): %d", len(a), len(b))
	}

	deltas := []string{}

	for i := range a {
		// This comparison is not ideal. The way the CostModelData endpoints work is that it calculates an offset based on the current time,
		// rather than accept a start and end time directly. Even worse, the new method of calculating the offset happens on the query arriving
		// to the http handler. In the old mechanism, it passed the offset all the way to prometheus. This means that the data is not guaranteed
		// to be the same, so we'll see if the timestamps are the same.
		aTs := time.Unix(int64(a[i].Timestamp), 0)
		bTs := time.Unix(int64(b[i].Timestamp), 0)

		dt := aTs.Sub(bTs).Abs()

		// only check the values if the timestamps are the same
		if dt == 0 {
			if !floatsEqual(a[i].Value, b[i].Value, valueEpsilon) {
				deltas = append(deltas, fmt.Sprintf("[%d] a.Value: %f != b.Value: %f - Delta: %f", i, a[i].Value, b[i].Value, math.Abs(a[i].Value-b[i].Value)))
			}
		}
	}

	return len(deltas) == 0, strings.Join(deltas, "\n")
}

func GetAllocations(t *testing.T, host string, start, end time.Time) map[string]model.AllocationResponseItem {
	t.Helper()

	url := fmt.Sprintf("%s/allocation/compute?window=%d-%d", host, start.Unix(), end.Unix())

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to get allocations: %s", err)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %s", err)
	}

	var parsedResp model.AllocationResponse

	err = json.Unmarshal(bytes, &parsedResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %s", err)
	}

	return parsedResp.Data[0]
}

func GetCostModelData(t *testing.T, host string, start, end time.Time) map[string]*costmodel.CostData {
	t.Helper()

	offsetSeconds := int(time.Since(end).Seconds())

	url := fmt.Sprintf("%s/costDataModel?timeWindow=1h&offset=%ds", host, offsetSeconds)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to get allocations: %s", err)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %s", err)
	}

	type rawResponse struct {
		Data map[string]*costmodel.CostData `json:"data"`
	}

	var parsedResp rawResponse

	err = json.Unmarshal(bytes, &parsedResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %s", err)
	}

	return parsedResp.Data
}

func GetAsset(t *testing.T, host string, start, end time.Time) map[string]model.AssetsResponseItem {
	t.Helper()

	url := fmt.Sprintf("%s/assets?window=%d-%d", host, start.Unix(), end.Unix())

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Failed to get allocations: %s", err)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %s", err)
	}

	var parsedResp model.AssetsResponse

	err = json.Unmarshal(bytes, &parsedResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %s", err)
	}

	return parsedResp.Data[0]
}
