// raft-reference runs deterministic test schedules against the pinned Go library.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type schedule struct {
	Name      string   `json:"name"`
	NodeCount int      `json:"nodeCount"`
	Voters    []uint64 `json:"voters"`
	Actions   []action `json:"actions"`
	// The C# test owns the independent checks; the reference just observes.
	Witnesses json.RawMessage `json:"witnesses"`
}

type action struct {
	Kind               string `json:"kind"`
	Node               uint64 `json:"node"`
	Index              uint64 `json:"index"`
	Text               string `json:"text"`
	PendingWorkAllowed bool   `json:"pendingWorkAllowed"`
}

type checkpoint struct {
	Label string    `json:"label"`
	Nodes []replica `json:"nodes"`
	Reads []read    `json:"reads"`
}

type replica struct {
	ID              uint64     `json:"id"`
	Term            uint64     `json:"term"`
	Vote            uint64     `json:"vote"`
	Leader          uint64     `json:"leader"`
	Commit          uint64     `json:"commit"`
	Applied         uint64     `json:"applied"`
	PhysicalApplied uint64     `json:"physicalApplied"`
	FirstIndex      uint64     `json:"firstIndex"`
	LastIndex       uint64     `json:"lastIndex"`
	Membership      membership `json:"membership"`
	Application     string     `json:"application"`
	Entries         []entry    `json:"entries"`
}

type membership struct {
	Voters         []uint64 `json:"voters"`
	VotersOutgoing []uint64 `json:"votersOutgoing"`
	Learners       []uint64 `json:"learners"`
	LearnersNext   []uint64 `json:"learnersNext"`
	AutoLeave      bool     `json:"autoLeave"`
}

type entry struct {
	Index uint64       `json:"index"`
	Term  uint64       `json:"term"`
	Type  pb.EntryType `json:"type"`
	Data  string       `json:"data"`
}

type read struct {
	Node    uint64 `json:"node"`
	Index   uint64 `json:"index"`
	Context string `json:"context"`
}

type node struct {
	id          uint64
	raw         *raft.RawNode
	storage     *raft.MemoryStorage
	physical    uint64
	config      *pb.ConfState
	application []byte
	entries     []entry
	appendWork  []*pb.Message
	applyWork   []*pb.Message
}

type cluster struct {
	async    bool
	isolate  uint64
	nodes    []node
	messages []*pb.Message
	reads    []read
}

func main() {
	if len(os.Args) != 3 || (os.Args[2] != "sync" && os.Args[2] != "async") {
		log.Fatal("usage: raft-reference <schedule> <sync|async>")
	}
	if os.Args[2] == "sync" && strings.HasPrefix(filepath.Base(os.Args[1]), "async-") {
		log.Fatal("this schedule requires async storage")
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var scenario schedule
	if err := decoder.Decode(&scenario); err != nil {
		log.Fatal(err)
	}
	_ = file.Close()
	result, err := run(scenario, os.Args[2] == "async")
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		log.Fatal(err)
	}
}

func run(s schedule, async bool) ([]checkpoint, error) {
	c, err := newCluster(s.NodeCount, s.Voters, async)
	if err != nil {
		return nil, err
	}
	var checkpoints []checkpoint
	for step, a := range s.Actions {
		if a.Kind == "checkpoint" {
			report, err := c.observe(a.Text, a.PendingWorkAllowed)
			if err != nil {
				return nil, err
			}
			checkpoints = append(checkpoints, report)
			continue
		}
		if err := c.act(a); err != nil {
			return nil, fmt.Errorf("%s action %d (%s): %w", s.Name, step+1, a.Kind, err)
		}
	}
	return checkpoints, nil
}

func newCluster(count int, voters []uint64, async bool) (*cluster, error) {
	c := &cluster{async: async, nodes: make([]node, count)}
	logger := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	for i := range c.nodes {
		id := uint64(i + 1)
		store := raft.NewMemoryStorage()
		conf := &pb.ConfState{Voters: slices.Clone(voters)}
		initial := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: new(uint64(2)), Term: new(uint64(1)), ConfState: conf,
		}}
		if err := store.ApplySnapshot(initial); err != nil {
			return nil, err
		}
		raw, err := raft.NewRawNode(&raft.Config{
			ID: id, Storage: store, Applied: 2, ElectionTick: 3, HeartbeatTick: 1,
			MaxSizePerMsg: math.MaxUint64, MaxInflightMsgs: math.MaxInt32,
			Logger: logger, AsyncStorageWrites: async,
		})
		if err != nil {
			return nil, err
		}
		c.nodes[i] = node{id: id, raw: raw, storage: store, physical: 2, config: conf}
	}
	return c, nil
}

func (c *cluster) act(a action) error {
	switch a.Kind {
	case "heal":
		c.isolate = 0
		return nil
	case "drain":
		return c.drain()
	}
	if a.Node == 0 || a.Node > uint64(len(c.nodes)) {
		return fmt.Errorf("invalid node %d", a.Node)
	}
	n := &c.nodes[a.Node-1]
	switch a.Kind {
	case "campaign":
		return n.raw.Campaign()
	case "heartbeat":
		n.raw.Tick()
	case "propose":
		return n.raw.Propose([]byte(a.Text))
	case "read":
		n.raw.ReadIndex([]byte(a.Text))
	case "add-learner":
		return n.raw.ProposeConfChange(&pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(a.Index)})
	case "cut":
		c.isolate = a.Node
	case "compact":
		if a.Index != n.physical {
			return fmt.Errorf("snapshot/compaction index %d does not match physical application %d", a.Index, n.physical)
		}
		if _, err := n.storage.CreateSnapshot(a.Index, n.config, slices.Clone(n.application)); err != nil {
			return err
		}
		return n.storage.Compact(a.Index)
	case "ready":
		if !n.raw.HasReady() {
			return fmt.Errorf("node %d has no Ready to process", n.id)
		}
		return c.ready(n)
	case "append-worker":
		if len(n.appendWork) == 0 {
			return fmt.Errorf("node %d has no append work", n.id)
		}
		return c.appendOne(n)
	case "deliver":
		delivered, err := c.deliverTo(n)
		if err != nil {
			return err
		}
		if !delivered {
			return fmt.Errorf("no messages to deliver to node %d", n.id)
		}
	default:
		return fmt.Errorf("unknown action %q", a.Kind)
	}
	return nil
}

// Visit Ready, deliveries, and each FIFO worker in the same order as the C# lab.
func (c *cluster) drain() error {
	for range 10_000 {
		worked := false
		for i := range c.nodes {
			n := &c.nodes[i]
			if n.raw.HasReady() {
				if err := c.ready(n); err != nil {
					return err
				}
				worked = true
			}
		}
		for i := range c.nodes {
			delivered, err := c.deliverTo(&c.nodes[i])
			if err != nil {
				return err
			}
			worked = worked || delivered
		}
		for i := range c.nodes {
			n := &c.nodes[i]
			for len(n.appendWork) > 0 {
				if err := c.appendOne(n); err != nil {
					return err
				}
				worked = true
			}
		}
		for i := range c.nodes {
			n := &c.nodes[i]
			for _, work := range n.applyWork {
				if err := n.apply(work.GetEntries()); err != nil {
					return err
				}
				c.messages = append(c.messages, work.GetResponses()...)
				worked = true
			}
			n.applyWork = nil
		}
		if !worked {
			return nil
		}
	}
	return errors.New("reference cluster did not quiesce within the drain budget")
}

func (c *cluster) deliverTo(n *node) (bool, error) {
	delivered := false
	for j := 0; j < len(c.messages); {
		message := c.messages[j]
		if message.GetTo() != n.id {
			j++
			continue
		}
		if err := c.deliver(n, message); err != nil {
			return false, err
		}
		c.messages = slices.Delete(c.messages, j, j+1)
		delivered = true
	}
	return delivered, nil
}

func (c *cluster) appendOne(n *node) error {
	work := n.appendWork[0]
	n.appendWork[0] = nil
	n.appendWork = n.appendWork[1:]
	var hard *pb.HardState
	if work.Term != nil {
		hard = &pb.HardState{Term: work.Term, Vote: work.Vote, Commit: work.Commit}
	}
	if err := n.persist(hard, work.GetEntries(), work.GetSnapshot()); err != nil {
		return err
	}
	c.messages = append(c.messages, work.GetResponses()...)
	return nil
}

func (c *cluster) ready(n *node) error {
	rd := n.raw.Ready()
	for _, state := range rd.ReadStates {
		c.reads = append(c.reads, read{n.id, state.Index, encode(state.RequestCtx)})
	}
	if !c.async {
		if err := n.persist(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
			return err
		}
		if err := n.apply(rd.CommittedEntries); err != nil {
			return err
		}
	}
	for _, message := range rd.Messages {
		switch message.GetTo() {
		case raft.LocalAppendThread:
			n.appendWork = append(n.appendWork, message)
		case raft.LocalApplyThread:
			n.applyWork = append(n.applyWork, message)
		default:
			c.messages = append(c.messages, message)
		}
	}
	if !c.async {
		n.raw.Advance(rd)
	}
	return nil
}

func (c *cluster) deliver(n *node, msg *pb.Message) error {
	from := msg.GetFrom()
	if from != n.id && !raft.IsLocalMsgTarget(from) && (from == c.isolate || n.id == c.isolate) {
		return nil
	}
	err := n.raw.Step(msg)
	if errors.Is(err, raft.ErrStepPeerNotFound) {
		// Same observable behavior for obsolete replies as in the C# lab.
		return nil
	}
	return err
}

func (n *node) persist(hard *pb.HardState, entries []*pb.Entry, snapshot *pb.Snapshot) error {
	if !raft.IsEmptySnap(snapshot) {
		if err := n.storage.ApplySnapshot(snapshot); err != nil {
			return err
		}
		n.physical = snapshot.GetMetadata().GetIndex()
		n.config = snapshot.GetMetadata().GetConfState()
		n.application = slices.Clone(snapshot.GetData())
	}
	if err := n.storage.Append(entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(hard) {
		return n.storage.SetHardState(hard)
	}
	return nil
}

func (n *node) apply(entries []*pb.Entry) error {
	for _, e := range entries {
		if e.GetIndex() <= n.physical {
			continue // an installed snapshot supersedes old worker effects
		}
		if e.GetIndex() != n.physical+1 {
			return fmt.Errorf("node %d: application gap from %d to %d", n.id, n.physical, e.GetIndex())
		}
		update := e.GetData()
		switch e.GetType() {
		case pb.EntryConfChange:
			var change pb.ConfChange
			if err := proto.Unmarshal(update, &change); err != nil {
				return err
			}
			n.config = n.raw.ApplyConfChange(&change)
			update = change.GetContext()
		case pb.EntryConfChangeV2:
			var change pb.ConfChangeV2
			if err := proto.Unmarshal(update, &change); err != nil {
				return err
			}
			n.config = n.raw.ApplyConfChange(&change)
			update = change.GetContext()
		}
		n.application = append(n.application, update...)
		n.physical = e.GetIndex()
		n.entries = append(n.entries, entry{e.GetIndex(), e.GetTerm(), e.GetType(), encode(e.GetData())})
	}
	return nil
}

func (c *cluster) observe(label string, pendingWorkAllowed bool) (checkpoint, error) {
	if !pendingWorkAllowed && len(c.messages) != 0 {
		return checkpoint{}, fmt.Errorf("%s: undelivered messages at checkpoint", label)
	}
	out := checkpoint{Label: label, Nodes: make([]replica, 0, len(c.nodes)), Reads: append([]read{}, c.reads...)}
	for i := range c.nodes {
		n := &c.nodes[i]
		if !pendingWorkAllowed && (n.raw.HasReady() || len(n.appendWork) != 0 || len(n.applyWork) != 0) {
			return checkpoint{}, fmt.Errorf("%s: undrained work at node %d", label, n.id)
		}
		status := n.raw.BasicStatus()
		first, err := n.storage.FirstIndex()
		if err != nil {
			return checkpoint{}, err
		}
		last, err := n.storage.LastIndex()
		if err != nil {
			return checkpoint{}, err
		}
		out.Nodes = append(out.Nodes, replica{
			ID: n.id, Term: status.GetTerm(), Vote: status.GetVote(), Leader: status.Lead, Commit: status.GetCommit(),
			Applied: status.Applied, PhysicalApplied: n.physical, FirstIndex: first, LastIndex: last,
			Membership: membership{
				Voters: ordered(n.config.GetVoters()), VotersOutgoing: ordered(n.config.GetVotersOutgoing()),
				Learners: ordered(n.config.GetLearners()), LearnersNext: ordered(n.config.GetLearnersNext()), AutoLeave: n.config.GetAutoLeave(),
			},
			Application: encode(n.application), Entries: append([]entry{}, n.entries...),
		})
		n.entries = nil
	}
	slices.SortFunc(out.Reads, func(a, b read) int {
		if a.Node != b.Node {
			if a.Node < b.Node {
				return -1
			}
			return 1
		}
		if rc := strings.Compare(a.Context, b.Context); rc != 0 {
			return rc
		}
		if a.Index < b.Index {
			return -1
		}
		if a.Index > b.Index {
			return 1
		}
		return 0
	})
	c.reads = nil
	return out, nil
}

func ordered(values []uint64) []uint64 {
	result := append([]uint64{}, values...)
	slices.Sort(result)
	return result
}

func encode(bytes []byte) string { return base64.StdEncoding.EncodeToString(bytes) }
