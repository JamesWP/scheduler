package simcluster

import (
	"encoding/json"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// defaultScheduler is what an unset pod.spec.schedulerName defaults to on
// a real apiserver create, and what the default scheduling profile below
// is named -- a pod addressed to any other name is silently ignored by
// this (or any) scheduler. The fake clientset has no create-time
// defaulting of its own, so callers building a Pod by hand have to set
// this themselves; store.py's pod-creation hook does the same for the
// Python backend, for the same reason (see README.md).
const defaultScheduler = "default-scheduler"

// nodeSpec/workloadSpec mirror the simplified nodes:/workloads: shape
// image/server/simulate.py accepts (see the top-level README's "Input
// format" section); a raw `kind: Node`/`kind: Pod` manifest is passed
// through untouched by decoding it directly into the target type instead.

func nodeFromSpec(spec map[string]any) (*corev1.Node, error) {
	if kindOf(spec) == "Node" {
		var node corev1.Node
		if err := decodeInto(spec, &node); err != nil {
			return nil, err
		}
		return &node, nil
	}

	name, _ := spec["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("node is missing a name")
	}
	cpu := stringOr(spec["cpu"], "4")
	memory := stringOr(spec["memory"], "8Gi")
	pods := stringOr(spec["pods"], "110")
	capacity, err := resourceList(map[string]string{"cpu": cpu, "memory": memory, "pods": pods})
	if err != nil {
		return nil, fmt.Errorf("node %s: %w", name, err)
	}

	labels := map[string]string{
		"type":                   "fake",
		"kubernetes.io/role":     "agent",
		"kubernetes.io/hostname": name,
	}
	for k, v := range stringMap(spec["labels"]) {
		labels[k] = v
	}

	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{
			Capacity:    capacity,
			Allocatable: capacity,
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}, nil
}

// podsFromSpec expands one workload into its replica Pods (name-0..n),
// or returns a single passed-through Pod manifest.
func podsFromSpec(spec map[string]any) ([]*corev1.Pod, error) {
	if kindOf(spec) == "Pod" {
		var pod corev1.Pod
		if err := decodeInto(spec, &pod); err != nil {
			return nil, err
		}
		if pod.Spec.SchedulerName == "" {
			pod.Spec.SchedulerName = defaultScheduler
		}
		return []*corev1.Pod{&pod}, nil
	}

	name, _ := spec["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("workload is missing a name")
	}
	replicas := intOr(spec["replicas"], 1)

	requests := map[string]string{}
	if v, ok := spec["cpu"]; ok {
		requests["cpu"] = stringOr(v, "")
	}
	if v, ok := spec["memory"]; ok {
		requests["memory"] = stringOr(v, "")
	}
	requestList, err := resourceList(requests)
	if err != nil {
		return nil, fmt.Errorf("workload %s: %w", name, err)
	}
	limitList, err := resourceList(stringMap(spec["limits"]))
	if err != nil {
		return nil, fmt.Errorf("workload %s limits: %w", name, err)
	}

	image := stringOr(spec["image"], "registry.k8s.io/pause:3.9")
	labels := stringMap(spec["labels"])
	labels["workload"] = name

	podSpec := corev1.PodSpec{
		SchedulerName: defaultScheduler,
		Containers: []corev1.Container{{
			Name:  "app",
			Image: image,
			Resources: corev1.ResourceRequirements{
				Requests: requestList,
				Limits:   limitList,
			},
		}},
	}
	if err := decodeField(spec["nodeSelector"], &podSpec.NodeSelector); err != nil {
		return nil, fmt.Errorf("workload %s nodeSelector: %w", name, err)
	}
	if err := decodeField(spec["affinity"], &podSpec.Affinity); err != nil {
		return nil, fmt.Errorf("workload %s affinity: %w", name, err)
	}
	if err := decodeField(spec["topologySpreadConstraints"], &podSpec.TopologySpreadConstraints); err != nil {
		return nil, fmt.Errorf("workload %s topologySpreadConstraints: %w", name, err)
	}
	if v, ok := spec["priorityClassName"].(string); ok {
		podSpec.PriorityClassName = v
	}
	if v, ok := spec["nodeName"].(string); ok {
		podSpec.NodeName = v
	}

	pods := make([]*corev1.Pod, replicas)
	for i := range pods {
		podLabels := make(map[string]string, len(labels))
		for k, v := range labels {
			podLabels[k] = v
		}
		pods[i] = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf("%s-%d", name, i),
				Labels: podLabels,
			},
			Spec: podSpec,
		}
	}
	return pods, nil
}

func kindOf(spec map[string]any) string {
	k, _ := spec["kind"].(string)
	return k
}

// decodeInto/decodeField round-trip through JSON to turn the loosely
// typed map[string]any the request body decoded into (a raw manifest, or
// one free-form spec field such as affinity) into the real, generated
// Kubernetes API type -- the same types the scheduler itself operates
// on, so there is no separate schema to keep in sync with upstream.
func decodeInto(spec map[string]any, out any) error {
	return decodeField(spec, out)
}

func decodeField(v any, out any) error {
	if v == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func resourceList(quantities map[string]string) (corev1.ResourceList, error) {
	list := corev1.ResourceList{}
	for name, qty := range quantities {
		if qty == "" {
			continue
		}
		q, err := resource.ParseQuantity(qty)
		if err != nil {
			return nil, fmt.Errorf("%s: %q: %w", name, qty, err)
		}
		list[corev1.ResourceName(name)] = q
	}
	return list, nil
}

func stringMap(v any) map[string]string {
	out := map[string]string{}
	m, _ := v.(map[string]any)
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

func stringOr(v any, def string) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		// A bare number in YAML/JSON (e.g. `cpu: 4`) decodes as float64;
		// format it back to plain decimal so resource.ParseQuantity sees
		// the same "4" the Python backend's str(spec.get(...)) would.
		return strconv.FormatFloat(t, 'f', -1, 64)
	case nil:
		return def
	default:
		return fmt.Sprint(t)
	}
}

func intOr(v any, def int) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case string:
		var n int
		if _, err := fmt.Sscanf(t, "%d", &n); err == nil {
			return n
		}
	}
	return def
}
