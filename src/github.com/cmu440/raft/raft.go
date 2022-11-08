//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"math/rand"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const NULL_INT = -99999 // NULL value for int types
const MAX_NUM_LOG_ENTRIES = 1000

const max_election_timeout = 600 // in ms
const min_election_timeout = 400
const heartbeat_interval = 120

// GetStateReply
// ========
// Struct to store the GetState() reply to client
type GetStateReply struct {
	me   int
	term int
	isleader	bool
}

//
// ApplyCommand
// ========
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
} // Response sent back to Leader

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.S
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	applyCh					chan ApplyCommand

	current_term			int
	current_term_mux			sync.Mutex
	voted_candidate_leader					int // peer's index that I voted for
	voted_for_mux				int // peer's index that I voted for

	// election_timeout_chan	chan bool // no need to implement your own timeouts around Call()
	// get_server_state			chan bool // 1: follower, 2: candidate, 3: leader
	// server_state_result			chan int // 1: follower, 2: candidate, 3: leader
	// set_server_state			chan int // 1: follower, 2: candidate, 3: leader
	server_state				int
	server_state_mux			sync.Mutex
	leader_id					int

	log_entries				[]LogEntry	// NOTE: log entries is a list in server but an Array in RPC Args
	num_logs 				int

	received_requestVote_chan		chan RequestVoteArgs
	requestVote_reply_chan          chan RequestVoteReply

	received_appendEntries_chan		chan AppendEntriesArgs
	appendEntries_reply_chan		chan AppendEntriesReply

	requestVote_rpcResult_chan      chan RequestVoteReply

	close_mainRoutine_chan	chan bool

	/* Channels for Client API calls */

	/* Volatile states */
	commit_index			int // index of highest log entry known to be committed (init to 0 -> incr)
	// ^ Log entry -> (index, term, command)
	last_applied			int	// index of highest log entry applied to state machine (init to 0 -> incr)
	/* Volatile states END*/

	/* Volatile states for Leader -> Re-init after election success */
	// next_index			Array[int] // index of next log entry to send to each Follower -> init to last_log_index + 1
	// match_index			Array[int] // index of highest log entry known to be commited on each Follower -> init to 0, incr monotonically
	/* Volatile states for Leader END */
}

/* Raft should maintain 2 properties
1. If 2 entries in diff logs have the same index and term -> command must be the same
2. If 2 entries in diff logs have the same index and term -> all prev entries are the same
*/

/*
On receive_AppendEntries -> Consistency Check:
if rf.log_entries[args.PrevLogIndex] != args.PrevLogTerm {
	return False // Leader will decrement nextIndex for this F
}
else { // Consistency Check passed -> time to update my logs to match leader
	for leader_entry, my_entry in zip(args.Entries, rf.log_entries[args.PrevLogIndex + 1: ]) {
		if leader_entry.Term != my_entry.Term {
			// discard my logs starting from this entry onwards

			// Append leader's entry and all that follows
		}
	}

	If leaderCommit > commitIndex {
		// Synchronize state machine with Leader's

		rf.commit_index = min(args.LeaderCommit, len(rf.log_entries) - 1)

		for i := rf.lastApplied + 1; i <= rf.commit_index; i++ {
			Apply command in rf.log_entries[i]
		}

		rf.last_applied = rf.commit_index
	}
}

On receive AppendEntries response:


On receive RequestVotes -> up-to-date check:
if args.LastLogTerm > rf.log_entries[-1].term {
	// Candidate is more up-to-date

} else if args.LastLogTerm == rf.log_entries[-1].term {
	// Check to see whose logs is longer
	num_logs = len
	my_lastLogIndex =
} else {

}
*/

func getRandomTime() time.Duration{
	timeout_duration := rand.Intn(max_election_timeout - min_election_timeout) + min_election_timeout
	return time.Duration(timeout_duration) * time.Millisecond
}

func (rf *Raft) followerRoutine(term int, voted_candidate_leader int) {  // Main Routine
	rf.mux.Lock()
	// fmt.Println(rf.me, "just became follower term:", term)
	rf.server_state = 1
	rf.current_term = term
	// me := rf.me
	rf.mux.Unlock()
	// rf.voted_candidate_leader = voted_candidate_leader
	var ticker *time.Ticker
	ticker = time.NewTicker(getRandomTime())
	for {
		select {
		case args := <- rf.received_requestVote_chan:
			// fmt.Println(rf.me, "F RequestVote received, term:", args.Term, args.Candidate_id)
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = NULL_INT
				rf.requestVote_reply_chan <- *reply

			} else if args.Term == term {
				if (voted_candidate_leader == NULL_INT || voted_candidate_leader == args.Candidate_id) {
					// Replace && true with candidate’s log is at least as up-to-date as receiver's log

					voted_candidate_leader = args.Candidate_id

					reply.Term = args.Term
					reply.Vote_granted = true
				} else {
					reply.Term = args.Term
					reply.Vote_granted = false // Alr voted for another peer or log is invalid
					reply.Voted_for = voted_candidate_leader
				}
				rf.requestVote_reply_chan <- *reply
			} else {
				// Case where args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log
				// term = args.Term
				// rf.voted_for = args.Candidate_id

				reply.Term = args.Term
				reply.Vote_granted = true
				rf.requestVote_reply_chan <- *reply

				// go rf.followerRoutine(args.Term, args.Candidate_id)
				// return
				rf.mux.Lock()
				// fmt.Println(rf.me, "just became follower term:", term)
				rf.server_state = 1
				rf.current_term = args.Term
				term = args.Term
				// me := rf.me
				rf.mux.Unlock()
				voted_candidate_leader = args.Candidate_id
			}
			// fmt.Println(me, "F RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
			ticker = time.NewTicker(getRandomTime())

		case args := <- rf.received_appendEntries_chan:
			// fmt.Println(me, "Follower RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)

			reply := &AppendEntriesReply{}
			if args.Term < term {
				// TODO: Add ||  log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				// Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply

			} else {
				if args.Log_entries != nil {
					// not HeartBeat -> Actual Log update
					// Do additional log checks here

				}
				reply.Success = true
				reply.Term = args.Term
				rf.appendEntries_reply_chan <- *reply

				if args.Term > term {
					// rf needs to store leaderId to redirect clients
					// go rf.followerRoutine(args.Term, args.Leader_id)
					// go rf.followerRoutine(args.Term, NULL_INT)
					// return
					rf.mux.Lock()
					// fmt.Println(rf.me, "just became follower term:", term)
					rf.server_state = 1
					rf.current_term = args.Term
					term = args.Term
					rf.mux.Unlock()
					voted_candidate_leader = NULL_INT
				}
			}

			ticker = time.NewTicker(getRandomTime())

		// case <- time.After(getRandomTime()):
		case <-ticker.C:
			// Election Timed Out //
			// fmt.Println(me, "F timeout", term)
			go rf.candidateRoutine(term + 1) // Become candidate
			return

		case <- rf.close_mainRoutine_chan:
			return
		}
	}
}

func candidateRequestVotes(requestVote_rpcResult_chan chan RequestVoteReply, term int, me int, rf *Raft) {
	for i:=0; i < len(rf.peers); i++ {
		/* Send RequestVote RPC in a new go routine */
		args := &RequestVoteArgs{}
		args.Candidate_id = me
		args.Term = term

		// fmt.Println(me, "C sending requestVote to", i, "term:", term)
		go func(peer_id int, args *RequestVoteArgs, requestVote_rpcResult_chan chan RequestVoteReply, ) {
			reply := &RequestVoteReply{}
			status := rf.sendRequestVote(peer_id, args, reply)
			// fmt.Println(me, "RequestVoteRPC reply received1", status, reply.Vote_granted, reply.Voted_for, reply.Term)
			if status == true {
				requestVote_rpcResult_chan <- *reply
			}
			return
		}(i, args, requestVote_rpcResult_chan)
	}
}

func (rf *Raft) candidateRoutine(term int) {
	rf.mux.Lock()
	// fmt.Println(rf.me, "just became candidate term:", term)
	rf.server_state = 2
	rf.current_term = term

	me := rf.me
	rf.mux.Unlock()

	// voted_candidate_leader := NULL_INT //  Vote for self
	var num_votes_granted int
	num_votes_granted = 0

	var requestVote_rpcResult_chan chan RequestVoteReply
	requestVote_rpcResult_chan = make(chan RequestVoteReply)
	// Send RequestVote RPCs to all other servers
	candidateRequestVotes(requestVote_rpcResult_chan, term, me, rf)

	var ticker *time.Ticker
	ticker = time.NewTicker(getRandomTime())

	for {
		select {
		case args := <- rf.received_requestVote_chan:
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = NULL_INT // Signify outdated ReqVote
				rf.requestVote_reply_chan <- *reply
				// fmt.Println(me, "C RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", rf.voted_for, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				// fmt.Println(me, "C: Candidate:", args.Candidate_id, "requestVote outdated", args.Term, term)
			} else if args.Term == term {
				// if rf.voted_for == args.Candidate_id {
				reply.Term = args.Term

				if args.Candidate_id == me {
					// Replace && true with candidate’s log is at least as up-to-date as receiver's log
					reply.Vote_granted = true
					// voted_candidate_leader = me
					// fmt.Println(me, "C RequestVote reply to myself: ", "voted_for:", rf.voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				} else {
					reply.Vote_granted = false // Alr voted for another peer or log is invalid
					reply.Voted_for = me
					// fmt.Println(me, "C RequestVote reply to Candidate:", args.Candidate_id, "imVotingForMyself voted_for:", voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				}
				rf.requestVote_reply_chan <- *reply
			} else {
				// args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log

				reply.Term = args.Term
				reply.Vote_granted = true
				rf.requestVote_reply_chan <- *reply

				// fmt.Println(me, "C RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", voted_candidate_leader, ": Reverting to Follower; args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				go rf.followerRoutine(args.Term, args.Candidate_id) // convert to follower
				return
			}

			// ticker = time.NewTicker(getRandomTime())

		case args := <- rf.received_appendEntries_chan:
			// fmt.Println(me, "Candidate RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)

			reply := &AppendEntriesReply{}
			if args.Term < term {
				// Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply
			} else {
				if args.Log_entries != nil {
					// not HeartBeat -> Actual Log update
					// Do additional log checks here
				}

				// if args.Term > term {
				if args.Term >= term {
					reply.Success = true
					reply.Term = args.Term
					rf.appendEntries_reply_chan <- *reply

					// term = args.Term
					// rf.leader_id = args.Leader_id
					go rf.followerRoutine(args.Term, args.Leader_id) // convert to follower
					return
				}
			}

			// ticker = time.NewTicker(getRandomTime())

		// case <- time.After(getRandomTime()):
		case <-ticker.C:
			// Election Timed Out //
			// fmt.Println(me, "C timeout", term)
			// go rf.candidateRoutine(term + 1)
			// return
			rf.mux.Lock()
			rf.server_state = 2
			rf.current_term = term + 1
			term += 1

			// me := rf.me
			rf.mux.Unlock()

			// voted_candidate_leader := NULL_INT //  Vote for self
			num_votes_granted = 0
			requestVote_rpcResult_chan = make(chan RequestVoteReply)
			candidateRequestVotes(requestVote_rpcResult_chan, term, me, rf)


		case reply := <- requestVote_rpcResult_chan:
			// rf.logger.Println("In Candidate requestVoteRPC response", term, reply.Term, reply.Vote_granted, reply.Voted_for)

			// Check reply & Process results

			if reply.Term > term {
				// convert to follower
				// fmt.Println(me, "C vote failed greater term, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
				go rf.followerRoutine(reply.Term, NULL_INT) // Set as NULL_INT not leader for now
				return
			} else if reply.Term == term {
				if reply.Vote_granted == true {
					num_votes_granted += 1
					// fmt.Println(me, "C vote success, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
					if num_votes_granted > len(rf.peers) / 2 {
						// Election Succeeded
						// fmt.Println(me, "Election Success", num_votes_granted, len(rf.peers))
						go rf.leaderRoutine(term)
						return
					}
				}
				// fmt.Println(me, "C vote failed firstcomefirstserve, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "F/C voted_for:", reply.Voted_for, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
			} else {
				rf.logger.Panicln("SOMETHING WEIRD HERE: requestVote rpc reply.Term < term", term, reply.Term)
			}

		case <- rf.close_mainRoutine_chan:
			return
		}
	}
}

func sendHeartBeat(appendEntries_rpcResult_chan chan AppendEntriesReply, term int, me int, rf *Raft) {
	// fmt.Println(me, "Leader sending Heartbeats, term:", term)
	for i:=0; i < len(rf.peers); i++ {
		if i != me {
			args := &AppendEntriesArgs{}
			args.Leader_id = me
			args.Term = term
			args.Log_entries = nil

			// fmt.Println(me, "sending HeartBeat to ", i)
			go func(peer_id int, args AppendEntriesArgs, appendEntries_rpcResult_chan chan AppendEntriesReply) {
				reply := &AppendEntriesReply{}
				status := rf.sendAppendEntries(peer_id, &args, reply)
				if status == true {
					appendEntries_rpcResult_chan <- *reply
				}
				return
			}(i, *args, appendEntries_rpcResult_chan)
		}
	}

	return
}

func sendAppendEntriesAll(appendEntries_rpcResult_chan chan AppendEntriesReply, nextIndices []int, matchIndex []int, term int, me int, rf *Raft) {
	// fmt.Println(me, "Leader sending Heartbeats, term:", term)
	last_log_id := get_last_log_id(rf)

	for i:=0; i < len(rf.peers); i++ {
		if i != me {
			next_index_i := nextIndices[i]
			args := &AppendEntriesArgs{}
			args.Leader_id = me
			args.Term = term

			if last_log_id >= next_index_i {
				// Send Append LogEntry //
				args.Log_entries = rf.log_entries[next_index_i:]

				if next_index_i == 1 {
					args.Prev_log_index = NULL_INT // NOTE: Receiver should check if log_index == NULL_INT -> always passes consistentCheck
					args.Prev_log_term = NULL_INT
				} else {
					args.Prev_log_index = next_index_i - 1
					args.Prev_log_term = rf.log_entries[args.Prev_log_index].term
				}

				args.Leader_commit_id = rf.commit_index
			} else {
				// Send HeartBeat //
				args.Log_entries = nil
			}

			// fmt.Println(me, "sending HeartBeat to ", i)
			go func(peer_id int, args AppendEntriesArgs, appendEntries_rpcResult_chan chan AppendEntriesReply) {
				reply := &AppendEntriesReply{}
				status := rf.sendAppendEntries(peer_id, &args, reply)
				if status == true {
					appendEntries_rpcResult_chan <- *reply
				}
				return
			}(i, *args, appendEntries_rpcResult_chan)
		}
	}

	return
}

func get_last_log_id(rf *Raft) int {
	rf.mux.Lock()
	last_log_id := rf.log_entries[rf.num_logs - 1].index
	if last_log_id != rf.num_logs {
		rf.logger.Panicln("last_log_id != rf.num_logs")
	}
	rf.mux.Unlock()

	return last_log_id
}

func (rf *Raft) leaderRoutine(term int) {
	rf.mux.Lock()
	rf.current_term = term
	// fmt.Println(rf.me, "just became leader term:", term)
	rf.server_state = 3

	me := rf.me

	var nextIndices []int
	var matchIndices []int

	last_log_id := get_last_log_id(rf)

	for i:=0; i < len(rf.peers); i++ {
		nextIndices = append(nextIndices, last_log_id + 1)
		matchIndices = append(matchIndices, 0)
	}

	rf.mux.Unlock()

	appendEntries_rpcResult_chan := make(chan AppendEntriesReply)

	// Send Heartbeats /* Send AppendEntries RPC in a new go routine */
	sendHeartBeat(appendEntries_rpcResult_chan, term, me, rf)

	var ticker *time.Ticker
	ticker = time.NewTicker(time.Duration(heartbeat_interval) * time.Millisecond)

	for {
		select {
		case args := <- rf.received_requestVote_chan:
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = 10000 // Signify I am the leader already
				rf.requestVote_reply_chan <- *reply
				// fmt.Println(me, "L RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)


			} else if args.Term == term {
				// if (rf.voted_for == NULL_INT || rf.voted_for == args.Candidate_id) && true {
				// 	// Replace && true with candidate’s log is at least as up-to-date as receiver's log

				// 	rf.voted_for = args.Candidate_id

				// 	reply.Term = args.Term
				// 	reply.Vote_granted = true
				// } else {
				reply.Term = args.Term
				reply.Vote_granted = false // Alr voted for another peer or log is invalid
				reply.Voted_for = 10000 // Signify I am the leader already
				// }
				rf.requestVote_reply_chan <- *reply
				// fmt.Println(me, "L RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", rf.voted_for, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

			} else {
				// args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log
				reply.Term = args.Term
				reply.Vote_granted = true
				rf.requestVote_reply_chan <- *reply

				// fmt.Println(me, "L RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", rf.voted_for, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				// fmt.Println(me, "lost leadership to candidate:", args.Candidate_id,"current_term:", term, "candidate term", args.Term)
				go rf.followerRoutine(args.Term, args.Candidate_id) // convert to follower
				return
			}

		case args := <- rf.received_appendEntries_chan:
			// fmt.Println(me, "LEADER RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)
			reply := &AppendEntriesReply{}
			if args.Term < term {
				// Sender Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply
			} else {

				if args.Log_entries != nil {
					// not HeartBeat -> Actual Log update
					// Do additional log checks here

				}
				reply.Success = true
				reply.Term = args.Term
				rf.appendEntries_reply_chan <- *reply

				if args.Term > term {
					// go rf.followerRoutine(args.Term, args.Leader_id) // convert to follower
					// fmt.Println(me, "lost leadership to another leader:", args.Leader_id,"current_term:", term, "candidate term", args.Term)
					go rf.followerRoutine(args.Term, NULL_INT) // convert to follower
					return

				} else if args.Term == term {
					if args.Leader_id != me {
						rf.logger.Println("2 Leaders in the same term -> have not decided how to handle", me,  args.Leader_id)
						// fmt.Println("2 Leaders in the same term -> have not decided how to handle", me,  args.Leader_id)
						// if args.Leader_id > me {
						// 	go rf.candidateRoutine(term + 1)  // Init new Election
						// 	return
						// }
						go rf.candidateRoutine(term + 1) // Init new Election
					}
					// Maybe break ties using leader_id and me
				}
			}

		case reply := <- appendEntries_rpcResult_chan:
			// Received Response from AppendEntries RPC call
			// fmt.Println(me, "L Heartbeat resp, term:", reply.Term, "success:", reply.Success)
			if reply.Success == false {
				if reply.Term > term {
					// Leader is outdated -> update term and switch to Follower
					// fmt.Println(me, "lost leadership during heartbeat response, current_term:", term, "candidate term", reply.Term)
					go rf.followerRoutine(reply.Term, NULL_INT)
					return
				} else {
					// Case where log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
					rf.logger.Println("Peer's prevLogTerm != Peer.log_entries[prevLogIndex]", term, reply.Term, reply.Success)
					// fmt.Println("Peer's prevLogTerm != Peer.log_entries[prevLogIndex]", term, reply.Term, reply.Success)
					// rf.logger.Panicln("Peer's prevLogTerm != Peer.log_entries[prevLogIndex]", term, reply.Term, reply.Success)
				}
			}

		// case <- time.After(time.Duration(heartbeat_interval) * time.Millisecond):
		case <-ticker.C:
			// Send Heartbeats
			// appendEntries_rpcResult_chan := make(chan AppendEntriesReply)
			// for i:=0; i < len(rf.peers); i++ {
			// 	/* Send AppendEntries RPC in a new go routine */
			// 	sendHeartBeat(appendEntries_rpcResult_chan, term, me, rf)
			// }
			// Send Heartbeats /* Send AppendEntries RPC in a new go routine */
			sendHeartBeat(appendEntries_rpcResult_chan, term, me, rf)

			// ticker = time.NewTicker(time.Duration(heartbeat_interval) * time.Millisecond)

		case <- rf.close_mainRoutine_chan:
			return
		}
	}
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	// rf.getState_called <- true
	// reply := <- rf.getState_reply
	rf.mux.Lock()
	me := rf.me
	current_term := rf.current_term
	server_state := rf.server_state
	rf.mux.Unlock()

	var isLeader bool
	if server_state == 3 {
		isLeader = true
	} else {
		isLeader = false
	}

	// fmt.Println("GET STATE RESULTS:", me, current_term, isLeader)

	return me, current_term, isLeader
}


type LogEntry struct {
	index 	int  // might need this if we need 1-indexing
	term		int
	Command interface{}
}

// AppendEntriesArgs
// ===============
// Example AppendEntries RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (2A, 2B)
	Term	int
	Leader_id	int

	Prev_log_index		int
	Prev_log_term		int

	Log_entries			[]LogEntry
	Leader_commit_id	int
}

// AppendEntriesReply
// ================
// Example AppendEntries RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A)
	Term			int	 // currentTerm for L to update itself
	Success			bool // True if follower contained entry matching prevLogIndex and prevLogTerm
}


// AppendEntries()
// ===============
// Example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
	1. Reply false if term < currentTerm
	2.
	*/
	rf.received_appendEntries_chan <- *args
	server_reply := <- rf.appendEntries_reply_chan

	reply.Term = server_reply.Term
	reply.Success = server_reply.Success
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// RequestVoteArgs
// ===============
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term	int
	Candidate_id	int

	Last_log_index	int
	Last_log_term	int
}

// RequestVoteReply
// ================
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A)
	Term			int	 // currentTerm for candidate to update itself
	Vote_granted	bool // True if granted, false if rejected
	Voted_for		int
}

// RequestVote
// ===========
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	/*
	1. Reply false if term < currentTerm
	2. If votedFor is null or candidateId, and candidate’s log is at
	least as up-to-date as receiver’s log, grant vote
	*/
	// fmt.Println(rf.me, "RequestVoteRPC handler called", "from:", args.Candidate_id, "term:", args.Term)
	rf.received_requestVote_chan <- *args
	// fmt.Println(rf.me, "RV RPC 2")
	server_reply := <-rf.requestVote_reply_chan

	reply.Term = server_reply.Term
	reply.Vote_granted = server_reply.Vote_granted

	return
}

// no need to implement your own timeouts around Call()
//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	/*  */
	// index := -1
	// term := -1
	// isLeader := true
	rf.mux.Lock()
	// Create new LogEntry and insert
	// get log index of new command
	current_term := rf.current_term
	server_state := rf.server_state

	if server_state != 3 {
		rf.mux.Unlock()
		return -1, current_term, false
	} else {
		// Get index for new log entry
		new_log_id := len(rf.log_entries) + 1

		new_log := &LogEntry{new_log_id, current_term, command}

		rf.log_entries = append(rf.log_entries, *new_log)
		rf.num_logs += 1

		rf.mux.Unlock()
		return new_log_id, current_term, false
	}
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	// Your code here, if desired
	rf.close_mainRoutine_chan <- true
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
//
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)
	// rf.current_term = 0
	// rf.voted_candidate_leader = NULL_INT
	// rf.server_state = 1 // servers init to follower
	// rf.leader_id = NULL_INT

	rf.applyCh = applyCh

	// rf.log_entries = make([]LogEntry, MAX_NUM_LOG_ENTRIES)
	rf.num_logs    = 0
	// rf.requestVote_reply_chan = make(chan RequestVoteReply)

	// rf.received_appendEntries_chan = make(chan AppendEntriesArgs)
	// rf.appendEntries_reply_chan = make(chan AppendEntriesReply)
	rf.received_requestVote_chan = make(chan RequestVoteArgs)
	rf.received_appendEntries_chan = make(chan AppendEntriesArgs)

	rf.requestVote_reply_chan = make(chan RequestVoteReply)
	rf.appendEntries_reply_chan = make(chan AppendEntriesReply)

	rf.close_mainRoutine_chan = make(chan bool)

	rf.commit_index	= 0
	rf.last_applied = 0

	// rf.next_index = NULL_INT
	// rf.match_index = NULL_INT

	rf.followerRoutine(0, NULL_INT)

	return rf
}
