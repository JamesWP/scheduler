package simcluster

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
)

// Row is one pod's final placement, mirroring the dicts
// image/server/simulate.py's run() yields in its "done" event's rows.
// Node is a pointer (JSON null, not an omitted key) because the CLI's
// presentation.py indexes rows["node"] unconditionally -- omitting the
// key on an unscheduled pod would be a KeyError there, not a graceful
// falsy value.
type Row struct {
	Pod      string  `json:"pod"`
	Node     *string `json:"node"`
	Status   string  `json:"status"`
	Workload string  `json:"workload"`
}

// Event is one NDJSON progress line, keyed the same way
// image/server/app.py's stream() forwards simulate.run()'s dicts --
// {"phase": ..., ...} -- so the existing schedsim CLI (schedsim/
// presentation.py) renders these without caring which backend produced
// them.
type Event map[string]any

// SimError is a run-ending condition to report to the caller, not a bug
// in this package -- e.g. nodes from an earlier, interrupted run still
// occupying the names this one wants. Mirrors simulate.py's SimError.
type SimError struct{ msg string }

func (e *SimError) Error() string { return e.msg }

// Run applies nodes then workloads, waits for the real scheduler to
// place every pod (or give up on it), and always cleans up afterwards
// unless keep is set. It streams progress on events (which Run closes
// when done) the same way simulate.run()'s generator does, and returns
// the final per-pod rows once the run (including cleanup) has finished.
func (c *Cluster) Run(ctx context.Context, nodeSpecs, workloadSpecs []map[string]any, timeout time.Duration, keep bool, events chan<- Event) ([]Row, error) {
	defer close(events)

	namespace := "sim-" + rand.String(8)
	var createdNodes []string
	var expected []string
	var rows []Row

	cleanup := func() {
		if keep {
			events <- Event{"phase": "kept", "namespace": namespace}
			return
		}
		started := time.Now()
		for i, name := range expected {
			_ = c.Client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
			emitProgress(events, "deleting pods", i+1, len(expected), started)
		}
		started = time.Now()
		for i, name := range createdNodes {
			_ = c.Client.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
			emitProgress(events, "deleting nodes", i+1, len(createdNodes), started)
		}
	}

	nodes := make([]*corev1.Node, 0, len(nodeSpecs))
	for _, spec := range nodeSpecs {
		node, err := nodeFromSpec(spec)
		if err != nil {
			cleanup()
			return nil, err
		}
		nodes = append(nodes, node)
	}
	started := time.Now()
	for i, node := range nodes {
		createdNodes = append(createdNodes, node.Name)
		if _, err := c.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			createdNodes = nil // don't delete nodes this run didn't create
			cleanup()
			if apierrors.IsAlreadyExists(err) {
				return nil, &SimError{fmt.Sprintf(
					"%v\nnodes from an earlier run are still in the cluster; "+
						"reset the server to clear them", err)}
			}
			return nil, err
		}
		emitProgress(events, "creating nodes", i+1, len(nodes), started)
	}

	var pods []*corev1.Pod
	for _, spec := range workloadSpecs {
		ps, err := podsFromSpec(spec)
		if err != nil {
			cleanup()
			return nil, err
		}
		pods = append(pods, ps...)
	}
	started = time.Now()
	for i, pod := range pods {
		expected = append(expected, pod.Name)
		if _, err := c.Client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			cleanup()
			return nil, err
		}
		emitProgress(events, "creating pods", i+1, len(pods), started)
	}

	rows, err := waitForScheduling(ctx, c.Client, namespace, expected, timeout, events)
	cleanup()
	if err != nil {
		return nil, err
	}
	events <- Event{"phase": "done", "rows": rows}
	return rows, nil
}

// emitProgress mirrors simulate.py's _apply_all: silent for small
// batches, one line every 100 items plus a final one carrying elapsed
// time -- the CLI only renders these for genuinely large runs.
func emitProgress(events chan<- Event, label string, done, total int, started time.Time) {
	if total == 0 || (done%100 != 0 && done != total) {
		return
	}
	e := Event{"phase": label, "done": done, "total": total}
	if done == total {
		e["elapsed"] = time.Since(started).Round(100 * time.Millisecond).Seconds()
	}
	events <- e
}

// waitForScheduling polls until every expected pod is bound or
// permanently unschedulable, exactly as simulate.py's
// _wait_for_scheduling does -- timeout is a no-progress budget, reset
// every time another pod settles, not a total deadline.
func waitForScheduling(ctx context.Context, client kubernetes.Interface, namespace string, expected []string, timeout time.Duration, events chan<- Event) ([]Row, error) {
	workloadOf := func(pod *corev1.Pod) string {
		if w := pod.Labels["workload"]; w != "" {
			return w
		}
		return pod.Name
	}

	deadline := time.Now().Add(timeout)
	best := 0
	rows := map[string]Row{}
	for time.Now().Before(deadline) {
		list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		rows = map[string]Row{}
		settled := true
		for i := range list.Items {
			pod := &list.Items[i]
			if pod.Spec.NodeName != "" {
				nodeName := pod.Spec.NodeName
				rows[pod.Name] = Row{Pod: pod.Name, Node: &nodeName,
					Status: "Scheduled", Workload: workloadOf(pod)}
				continue
			}
			if reason, ok := unschedulableReason(pod); ok {
				rows[pod.Name] = Row{Pod: pod.Name,
					Status: "Unschedulable: " + reason, Workload: workloadOf(pod)}
			} else {
				settled = false
			}
		}
		if settled && len(rows) == len(expected) {
			if best > 0 {
				events <- Event{"phase": "scheduling", "done": len(expected), "total": len(expected)}
			}
			break
		}
		if len(rows) > best {
			best = len(rows)
			deadline = time.Now().Add(timeout)
			events <- Event{"phase": "scheduling", "done": best, "total": len(expected)}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if len(rows) != len(expected) {
		events <- Event{"phase": "scheduling timed out", "done": len(rows), "total": len(expected),
			"message": fmt.Sprintf("gave up after %s with no further progress", timeout)}
	}

	out := make([]Row, len(expected))
	for i, name := range expected {
		if row, ok := rows[name]; ok {
			out[i] = row
		} else {
			out[i] = Row{Pod: name, Status: "Pending (timed out)", Workload: name}
		}
	}
	return out, nil
}

// unschedulableReason reads the same PodScheduled=False condition the
// real scheduler's handleSchedulingFailure patches onto the pod
// (pkg/scheduler/schedule_one.go) -- the message is the actual FitError
// text from the real filter plugins, verbatim.
func unschedulableReason(pod *corev1.Pod) (string, bool) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
			if cond.Message != "" {
				return cond.Message, true
			}
			return string(cond.Reason), true
		}
	}
	return "", false
}
