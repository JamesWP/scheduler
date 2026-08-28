// Package simcluster is the entire seam between this alternative backend
// and the real kube-scheduler codebase.
//
// It replaces image/server/{store.py,fakeapi.py} -- the in-memory object
// store plus the HTTP layer that speaks enough of the Kubernetes REST/watch
// API for an external kube-scheduler *binary* to treat it as a real
// apiserver -- with a fake client-go clientset. Nothing here reimplements
// any scheduling behaviour: Cluster wires up client-go's own
// fake.Clientset as the object store and k8s.io/kubernetes/pkg/scheduler's
// own Scheduler as the thing making decisions against it, exactly as
// upstream's own tests do (see pkg/scheduler/scheduler_test.go). The only
// custom code below is:
//
//   - a reactor translating the pods/binding subresource create the
//     scheduler issues into a plain pod.Spec.NodeName write, because
//     client-go's fake ObjectTracker has no built-in notion of binding
//     (mirrors store.py's bind_pod for the same reason);
//   - assigning UIDs on create, because unlike a real apiserver the fake
//     clientset never does (and the scheduler's internal queues key
//     pods by UID).
//
// Everything else -- filtering, scoring, preemption, the default profile,
// patching PodScheduled=False with the real unschedulable reason -- runs
// as the actual upstream scheduler code, unmodified, against the fake
// clientset. Keeping this package small and free of scheduling logic is
// what keeps it cheap to carry forward across kube-scheduler versions:
// bumping the version pinned in go.mod is the expected update path, the
// same way image/Dockerfile pins the binary today.
package simcluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

var podsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

// Cluster is one long-lived fake control plane: a fake clientset standing
// in for etcd/apiserver, and a real scheduler.Scheduler running its
// ScheduleOne loop against it. It plays the same role as the container in
// the Python design (image/server/app.py) -- started once, reused across
// many /run calls -- except there's no separate OS process or HTTP hop for
// the scheduler to reach the store through: it's the same clientset
// interface either way, just backed by a fake instead of a real apiserver.
type Cluster struct {
	Client kubernetes.Interface
}

// New starts the fake clientset, the scheduler, and its informers, and
// returns once the informer caches have synced. ctx controls the
// scheduler's lifetime; cancel it to shut the whole control plane down.
func New(ctx context.Context) (*Cluster, error) {
	client := fakeclientset.NewClientset()
	addBindingReactor(client)
	addUIDReactor(client)

	informerFactory := informers.NewSharedInformerFactory(client, 0)
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})

	sched, err := scheduler.New(
		ctx,
		client,
		informerFactory,
		nil, // dynamic informer factory: only needed for DynamicResourceAllocation, unused here
		profile.NewRecorderFactory(broadcaster),
		// No further options: this is the same "leave everything at its
		// default" call upstream's own tests make, which is what pulls in
		// the real default scheduling profile (pkg/scheduler/apis/config/v1's
		// getDefaultPlugins) rather than anything hand-picked here.
	)
	if err != nil {
		return nil, fmt.Errorf("building scheduler: %w", err)
	}

	broadcaster.StartRecordingToSink(ctx.Done())
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	go sched.Run(ctx)

	return &Cluster{Client: client}, nil
}

// addBindingReactor teaches the fake clientset what a pods/binding create
// means. A real apiserver's binding registry does this as one step of
// handling the write (set spec.nodeName, nothing else -- no real kubelet
// here to report back a Ready pod); ObjectTracker has no equivalent
// built in, so this is the one bit of "fake apiserver" behaviour this
// package still has to supply itself. Directly analogous to store.py's
// bind_pod, and just as small.
func addBindingReactor(client *fakeclientset.Clientset) {
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil // not a bind: fall through to the default create reactor
		}
		binding, ok := action.(clienttesting.CreateAction).GetObject().(*corev1.Binding)
		if !ok {
			return false, nil, nil
		}
		obj, err := client.Tracker().Get(podsGVR, binding.Namespace, binding.Name)
		if err != nil {
			return true, nil, err
		}
		pod := obj.(*corev1.Pod).DeepCopy()
		pod.Spec.NodeName = binding.Target.Name
		if err := client.Tracker().Update(podsGVR, pod, binding.Namespace); err != nil {
			return true, nil, err
		}
		return true, binding, nil
	})
}

// addUIDReactor assigns a UID on every create, same as a real apiserver's
// object creation strategy would. The fake ObjectTracker never does this
// on its own, but the scheduler keys its internal queues and caches off
// pod.UID -- leaving it blank would make every pod collide with every
// other one.
func addUIDReactor(client *fakeclientset.Clientset) {
	client.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(clienttesting.CreateAction)
		if !ok || action.GetSubresource() != "" {
			return false, nil, nil
		}
		obj, ok := create.GetObject().(metav1.Object)
		if !ok || obj.GetUID() != "" {
			return false, nil, nil
		}
		obj.SetUID(uuid.NewUUID())
		return false, nil, nil // mutated in place; let the default reactor still do the create
	})
}
