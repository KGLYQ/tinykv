// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//TODO:暂时不考虑snapshot
	return uint64(len(l.entries) - 1)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	entry := l.entries[i]
	return entry.Term, nil
}

func (l *RaftLog) slice(start uint64) []*pb.Entry {
	if start > l.LastIndex() {
		panic("idx err...")
	}
	arr := make([]*pb.Entry, 0)
	for _, log := range l.entries[start:] {
		arr = append(arr, &log)
	}
	return arr
}

func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) bool {
	term, _ := l.Term(index)
	if term != logTerm {
		return false
	}

	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) index() {

}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	//更新/覆盖本地日志，不能直接删除后面的日志
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
	//and truncating the log would mean “taking back” entries that we may have already told the leader that we have in our log.
	idx := ents[0].Index
	for _, entry := range ents {
		//覆盖
		if idx <= l.LastIndex() {
			if l.index(idx).Term != entry.Term {
				DPrintf("log conflict,delete log...peerId:%d,delete start:%d", rf.me, idx)
				rf.logType.trimLast(idx)
				//rf.log = rf.log[0:idx]
				//rf.log = append(rf.log, entry)
				rf.logType.append(entry)
			}
		} else {
			rf.logType.append(entry)
		}
		idx++
	}
	l.unstable.truncateAndAppend(ents)
	return l.LastIndex()
}
