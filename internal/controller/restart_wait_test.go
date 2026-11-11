package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kividbio/kividb-operator/internal/agentapi"
)

// A master whose pod is gone, and whose engine no longer answers, may be
// coming back on its volume, with writes no replica has (#78).

// newGoneMasterHarness: the master c1-0 is gone -- its pod object, and its
// engine with it -- after acknowledging writes on stream-a; its replicas
// hold 5 keys each. The pass is timed by *now.
func newGoneMasterHarness(t *testing.T) (*rolesHarness, *time.Time) {
	t.Helper()
	h := newFenceHarness(t)
	delete(h.agents.status, "10.0.0.1")
	h.cluster.Status.Pods[0].ReplID = "stream-a"
	keyed(h.agents.status["10.0.0.2"], 5)
	keyed(h.agents.status["10.0.0.3"], 5)
	now := time.Now()
	h.r.clock = func() time.Time { return now }
	return h, &now
}

// backOnItsVolume is c1-0's engine started again from its append-only file,
// in a new stream, holding keys keys.
func backOnItsVolume(keys int64) *agentapi.StatusResponse {
	st := keyed(masterRun(0, "stream-b", "run-new"), keys)
	st.AofEnabled, st.AofAckAfterWrite, st.EngineVersion = true, "yes", "1.2.0"
	return st
}

// noRestartWait turns the wait for a master to come back off, for a test of
// what follows it.
func (h *rolesHarness) noRestartWait() {
	off := int32(0)
	h.cluster.Spec.Failover.RestartWaitSeconds = &off
}

func (h *rolesHarness) assertWaiting(when string) {
	h.t.Helper()
	if _, _, _, err := h.r.reconcileRoles(context.Background(), h.cluster, h.pods()); err == nil {
		h.t.Fatalf("%s: reconcileRoles succeeded, want it waiting for the master", when)
	}
	h.assertNoCall("promote")
}

// It is waited for, for restartWaitSeconds (15 s by default), and then
// failed away from, each said in an event.
func TestFailover_WaitsForAMasterWhosePodIsGone(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(10 * time.Second)
	h.assertWaiting("10 s later")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "15s") {
		t.Errorf("events %v, want one FailoverWait saying it waits up to 15s", e)
	}
	*now = now.Add(6 * time.Second)
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "did not come back") {
		t.Errorf("events %v, want one FailoverWait saying it did not come back", e)
	}
}

// It comes back holding at least the keys its replicas hold, with the AOF
// acknowledging only what is in the file: it stays the master, as a master
// that restarted in place does (#42), and the replicas follow it.
func TestFailover_KeepsAMasterThatCameBackOnItsVolume(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.assertWaiting("at once")
	*now = now.Add(3 * time.Second)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(5)})
	if m, failover := h.reconcile(); m != "c1-0" || failover {
		t.Fatalf("master=%q failover=%v, want c1-0 kept", m, failover)
	}
	h.assertNoCall("promote")
	h.assertRole("10.0.0.2", agentapi.RoleReplica, "10.0.0.9")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 2 || !strings.Contains(e[1], "came back") {
		t.Errorf("events %v, want FailoverWait saying it came back", e)
	}
}

// It comes back with less than its replicas hold: the replica that holds
// the most takes over, as from a master that restarted behind them.
func TestFailover_FailsOverFromAMasterThatCameBackBehind(t *testing.T) {
	h, now := newGoneMasterHarness(t)
	h.assertWaiting("at once")
	*now = now.Add(3 * time.Second)
	h.addPod(testPod{name: "c1-0", ip: "10.0.0.9", label: "master", ready: true, uid: "new", agent: backOnItsVolume(2)})
	h.assertFailoverTo("c1-1")
}

// failover.restartWaitSeconds: 0 turns the wait off.
func TestFailover_DoesNotWaitWithRestartWaitOff(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	off := int32(0)
	h.cluster.Spec.Failover.RestartWaitSeconds = &off
	h.assertFailoverTo("c1-1")
}

// A master whose engine still answers is held by the fence, not waited for.
func TestFailover_DoesNotWaitForAMasterThatStillAnswers(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.agents.status["10.0.0.1"] = masterRun(100, "stream-a", "run-0")
	h.assertFailoverTo("c1-1")
	h.assertCallOrder("exec 10.0.0.1 CLIENT PAUSE 65000 WRITE", "promote 10.0.0.2")
}

// Its replacement pod cannot be scheduled -- its volume is pinned to a node
// that is not Ready, say: it is not coming back soon, and waiting would only
// add downtime.
func TestFailover_DoesNotWaitForAPodThatCannotBeScheduled(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	rec := h.recordEvents()
	h.addPod(testPod{name: "c1-0", uid: "new", unschedulable: true})
	h.assertFailoverTo("c1-1")
	if e := eventsWith(rec, "FailoverWait"); len(e) != 1 || !strings.Contains(e[0], "cannot be scheduled") {
		t.Errorf("events %v, want one FailoverWait saying why it did not wait", e)
	}
}

// A replacement pod with no Ready condition yet is still on its way: the
// failover waits for it, as for a pod that is gone.
func TestFailover_WaitsForAReplacementPodNotStartedYet(t *testing.T) {
	h, _ := newGoneMasterHarness(t)
	h.addPod(testPod{name: "c1-0", uid: "new", pending: true})
	h.assertWaiting("at once")
}
