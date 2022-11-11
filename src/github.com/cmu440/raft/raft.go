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
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const NULL_INT = -99999 // NULL value for int types
const MAX_ARG_LOG_ENTRIES = 20

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

func printLogs(me int, log_entries []LogEntry, rf *Raft) {
	rf.logger.Println("rf", me, ": Printing logs |")
	for i, log := range log_entries{
		// log := log_entries[i]
		rf.logger.Println("rf", me, "|", i, "| index", log.Index, "term:", log.Term, "command:", log.Command)
	}
	rf.logger.Println(me, ": Print logs END |")
}

func get_last_log_id(rf *Raft) int {
	var last_log_id int
	rf.mux.Lock()
	if len(rf.log_entries) == 0 {
		last_log_id = NULL_INT
	} else {
		last_log_id = rf.log_entries[rf.num_logs - 1].Index
		if last_log_id != rf.num_logs {
			rf.logger.Panicln("last_log_id != rf.num_logs")
		}
	}
	rf.mux.Unlock()

	return last_log_id
}

func get_last_log_term(rf *Raft) int {
	var last_log_term int
	rf.mux.Lock()
	if len(rf.log_entries) == 0 {
		last_log_term = NULL_INT
	} else {
		last_log_id := rf.log_entries[rf.num_logs - 1].Index
		if last_log_id != rf.num_logs {
			rf.logger.Panicln("last_log_id != rf.num_logs")
		}
		last_log_term = rf.log_entries[rf.num_logs - 1].Term
	}
	rf.mux.Unlock()

	return last_log_term
}

func get_upToDate_check(args RequestVoteArgs, rf *Raft) bool {
	last_log_id := get_last_log_id(rf)
	var my_last_log_term int
	if last_log_id == NULL_INT { // my logs are empty
		my_last_log_term = NULL_INT
	} else {
		rf.mux.Lock()
		my_last_log_term = rf.log_entries[last_log_id - 1].Term
		rf.mux.Unlock()
	}

	rf.logger.Println("Up-to-date check. Arg-lastLogIndex:", args.Last_log_index, "Arg-lastLogTerm", args.Last_log_term, "myLastLogIndex", last_log_id, "myLastLogTerm:", my_last_log_term)

	if args.Last_log_term == NULL_INT && my_last_log_term == NULL_INT {
		// both candidate and receiver's logs are empty
		return true
	}

	if args.Last_log_term > my_last_log_term {
		return true // Candidate is more up-to-date
	} else if args.Last_log_term == my_last_log_term {
		// Check to see whose logs is longer
		if args.Last_log_index > last_log_id {
			return true // Candidate is more up-to-date
		} else if args.Last_log_index < last_log_id {
			return false // i am more up-to-date
		} else {
			rf.logger.Println("Weird case: args.LastLogIndex == last_log_id", args.Last_log_index, last_log_id)
			return true
		}
	} else {
		return false // i am more up-to-date
	}
}

func HandleAppendEntry(me int, term int, args *AppendEntriesArgs, reply *AppendEntriesReply, rf *Raft) {
	if args.Is_heartbeat == false {
		//  Actual Log update //
		rf.mux.Lock()
		var my_prev_log_id int
		var my_prev_log_term int
		if args.Prev_log_index != NULL_INT && len(rf.log_entries) >= args.Prev_log_index {
			my_prev_log_entry := &rf.log_entries[args.Prev_log_index - 1]
			my_prev_log_id = my_prev_log_entry.Index
			my_prev_log_term = my_prev_log_entry.Term
		} else {
			// my_log_prev_log_id = nil
			my_prev_log_id = NULL_INT
			my_prev_log_term = NULL_INT
		}
		rf.mux.Unlock()

		/* Consistency Check */
		if (my_prev_log_id == NULL_INT && args.Prev_log_index == NULL_INT) || (my_prev_log_id == args.Prev_log_index && my_prev_log_term == args.Prev_log_term) {
			/* Consistency Check passed
			1. Check for Conflicts and discard
			2. Update Commit_index
			3. for-loop over log_entries and apply commands
			4. Reply Success
			*/
			rf.mux.Lock()
			log_entries := rf.log_entries
			num_logs := rf.num_logs

			if args.Prev_log_index != NULL_INT {
				rf.logger.Println(me, "Checking for conflicts", len(args.Log_entries), "args.Prev_log_index", args.Prev_log_index)
				// printLogs(me , args.Log_entries[:])
					// rf.logger.Println(me, "args.Prev_log_index", args.Prev_log_index)
				// 1. Check for Conflicts and discard
				i := 0
				for {
					my_log_0_id := i + args.Prev_log_index
					if my_log_0_id >= num_logs {
						// reached the end of my logs
						// for j := i; j < len(args.Log_entries); j++ {
						// 		rf.logger.Println(me, "log appending", j, "index:", args.Log_entries[j].Index, "term:", args.Log_entries[j].Term)
						// 	log_entries = append(log_entries, args.Log_entries[j])
						// } // Append leader's entry and all that follows
						log_entries = append(log_entries, args.Log_entries[i:]...)
						break
					}
					// rf.logger.Println(me, "F passed consistencyCheck my_log_0_id", my_log_0_id, "Prev_log_index", args.Prev_log_index, "term", term)

					// Check for Conflicts
					if log_entries[my_log_0_id].Term != args.Log_entries[i].Term {
						log_entries = log_entries[:my_log_0_id] // discard my logs starting from this entry

						// for j := i; j < len(args.Log_entries); j++ {
						// 		rf.logger.Println(me, "log appending", j, "index:", args.Log_entries[j].Index, "term:", args.Log_entries[j].Term)
						// 	log_entries = append(log_entries, args.Log_entries[j])
						// } // Append leader's entry and all that follows
						log_entries = append(log_entries, args.Log_entries[i:]...)
						break
					}

					i += 1
				} //
			} else {
				// for j := 0; j < len(args.Log_entries); j++ {
				// 		rf.logger.Println(me, "log appending", j, "index:", args.Log_entries[j].Index, "term:", args.Log_entries[j].Term)
				// 	log_entries = append(log_entries, args.Log_entries[j])
				// } // Append leader's entry and all that follows
				// rf.logger.Println(me, "leader:", args.Leader_id, "args.Prev_log_index is null", len(args.Log_entries), "args.Prev_log_index", args.Prev_log_index)
				log_entries = append(log_entries, args.Log_entries...)
			}

			num_logs = len(log_entries)
			if num_logs != log_entries[num_logs - 1].Index {
					// printLogs(me, log_entries)
				// rf.logger.Println(me, "")
				rf.logger.Panicln("Error in", me, "F receive AppendEntries: num_logs != log_entries[num_logs - 1].Index", num_logs, log_entries[num_logs - 1].Index)
			}
			rf.log_entries = log_entries
			rf.num_logs = len(log_entries)
			rf.mux.Unlock()

			// 2. Update commit index to min(leaderCommit, index of last new entry)
			if args.Leader_commit_id > rf.commit_index {
				if args.Leader_commit_id > num_logs {
					// rf.logger.Println(me, "updating commit_index with num_logs", num_logs, "rf.commit_index", rf.commit_index, "num_logs", num_logs)
					rf.commit_index = num_logs
				} else {
					// rf.logger.Println(me, "updating commit_index with args.Leader_commit_id", "rf.commit_index", rf.commit_index, "args.Leader_commit_id", args.Leader_commit_id)
					rf.commit_index = args.Leader_commit_id
				}
			} else {
				// rf.logger.Println(me, "args.Leader_commit_id < rf.commit_index updating commit_index with num_logs", num_logs, "rf.commit_index", rf.commit_index, "args.Leader_commit_id", args.Leader_commit_id)
				rf.commit_index = num_logs
			}

			// 3. for-loop over log_entries and apply commands
			if rf.commit_index > rf.last_applied {
				for i := rf.last_applied; i < rf.commit_index; i++ {
					log := log_entries[i]
					apply_command := &ApplyCommand{log.Index, log.Command}
					rf.logger.Println(me, "APPLYING COmmand, index:", log.Index, "term:", log.Term, "Command:", log.Command)
					rf.applyCh <- *apply_command
				}
				rf.last_applied = rf.commit_index
			} //
			// printLogs(me, log_entries)

			// 4. Reply Success
			reply.New_match_id = rf.commit_index
			reply.Success = true
			reply.Term = args.Term //
		} else {
			reply.Success = false // Consistency Check fails
			reply.Term = args.Term
		}
		reply.Is_heartbeat = false
		/* Consistency Check END */

	} else {
		// Regular HeartBeat response to args.Term >= term
		reply.Is_heartbeat = true
		reply.New_match_id = NULL_INT
		reply.Success = true
		reply.Term = args.Term
	}

	reply.From = me
	// rf.logger.Println(me, "replying leader:", args.Leader_id, "match_ID", reply.New_match_id, "rf.commit_index", rf.commit_index, "rf.num_logs", rf.num_logs)
	rf.appendEntries_reply_chan <- *reply
}

func getRandomTime() time.Duration{
	timeout_duration := rand.Intn(max_election_timeout - min_election_timeout) + min_election_timeout
	return time.Duration(timeout_duration) * time.Millisecond
}

func (rf *Raft) followerRoutine(term int, voted_candidate_leader int) {  // Main Routine
	rf.mux.Lock()
	rf.logger.Println(rf.me, "just became follower term:", term)
	rf.server_state = 1
	rf.current_term = term
	me := rf.me
	rf.mux.Unlock()
	// rf.voted_candidate_leader = voted_candidate_leader
	var ticker *time.Ticker
	ticker = time.NewTicker(getRandomTime())
	for {
		select {
		case args := <- rf.received_requestVote_chan:
			// rf.logger.Println(rf.me, "F RequestVote received, term:", args.Term, args.Candidate_id)
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = NULL_INT
				rf.requestVote_reply_chan <- *reply
				rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "F falied-lowerTerm, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

			} else if args.Term == term {
				if (voted_candidate_leader == NULL_INT || voted_candidate_leader == args.Candidate_id) && get_upToDate_check(args, rf) {
					// Replace && true with candidate’s log is at least as up-to-date as receiver's log

					voted_candidate_leader = args.Candidate_id

					reply.Term = args.Term
					reply.Vote_granted = true
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "F success-voteForFirstEqualTerm, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				} else {
					reply.Term = args.Term
					reply.Vote_granted = false // Alr voted for another peer or log is invalid
					reply.Voted_for = voted_candidate_leader

					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "F falied-alreadyVotedEqualTerm, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				}
				rf.requestVote_reply_chan <- *reply
			} else {
				// Case where args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log
				// term = args.Term
				// rf.voted_for = args.Candidate_id

				reply.Term = args.Term
				if get_upToDate_check(args, rf) == true {
					reply.Vote_granted = true
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "F success-TermHigher-UpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				} else {
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "F failed-NotUpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					reply.Vote_granted = false
				}

				rf.requestVote_reply_chan <- *reply

				// go rf.followerRoutine(args.Term, args.Candidate_id)
				// return
				rf.mux.Lock()
				// rf.logger.Println(rf.me, "just became follower term:", term)
				rf.server_state = 1
				rf.current_term = args.Term
				term = args.Term
				// me := rf.me
				rf.mux.Unlock()
				voted_candidate_leader = args.Candidate_id
			}
			// rf.logger.Println(me, "F RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
			ticker = time.NewTicker(getRandomTime())

		case args := <- rf.received_appendEntries_chan:
			// rf.logger.Println(me, "Follower RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)

			reply := &AppendEntriesReply{}
			if args.Term < term {
				// Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply

			} else {
				HandleAppendEntry(me, term, &args, reply, rf)

				if args.Term > term {
					// rf needs to store leaderId to redirect clients
					rf.mux.Lock()
					// rf.logger.Println(rf.me, "just became follower term:", term)
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
			// rf.logger.Println(me, "F timeout", term)
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

		args.Last_log_index = get_last_log_id(rf)
		args.Last_log_term = get_last_log_term(rf)

		// rf.logger.Println(me, "C sending requestVote to", i, "term:", term)
		go func(peer_id int, args *RequestVoteArgs, requestVote_rpcResult_chan chan RequestVoteReply, ) {
			reply := &RequestVoteReply{}
			status := rf.sendRequestVote(peer_id, args, reply)
			// rf.logger.Println(me, "RequestVoteRPC reply received1", status, reply.Vote_granted, reply.Voted_for, reply.Term)
			if status == true {
				requestVote_rpcResult_chan <- *reply
			}
			return
		}(i, args, requestVote_rpcResult_chan)
	}
}

func (rf *Raft) candidateRoutine(term int) {
	rf.mux.Lock()
	rf.logger.Println(rf.me, "just became candidate term:", term)
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
			// rf.logger.Println(rf.me, "C RequestVote received, term:", args.Term, args.Candidate_id)
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = NULL_INT // Signify outdated ReqVote
				rf.requestVote_reply_chan <- *reply
				rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "C falied-lowerTerm, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				// rf.logger.Println(me, "C RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", rf.voted_for, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				// rf.logger.Println(me, "C: Candidate:", args.Candidate_id, "requestVote outdated", args.Term, term)
			} else if args.Term == term {
				// if rf.voted_for == args.Candidate_id {
				reply.Term = args.Term

				if args.Candidate_id == me {
					// Replace && true with candidate’s log is at least as up-to-date as receiver's log
					reply.Vote_granted = true
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "C success-voteForSelf, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					// voted_candidate_leader = me
					// rf.logger.Println(me, "C RequestVote reply to myself: ", "voted_for:", rf.voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				} else {
					reply.Vote_granted = false // Alr voted for another peer or log is invalid
					reply.Voted_for = me
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "C falied-voteForSelf, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					// rf.logger.Println(me, "C RequestVote reply to Candidate:", args.Candidate_id, "imVotingForMyself voted_for:", voted_candidate_leader, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
				}
				rf.requestVote_reply_chan <- *reply
			} else {
				// args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log
				reply.Term = args.Term
				if get_upToDate_check(args, rf) == true {
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "C success-TermHigher-UpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					reply.Vote_granted = true
				} else {
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "C failed-NotUpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					reply.Vote_granted = false
				}
				rf.requestVote_reply_chan <- *reply

				go rf.followerRoutine(args.Term, args.Candidate_id) // convert to follower
				return
			}
			// ticker = time.NewTicker(getRandomTime())

		case args := <- rf.received_appendEntries_chan:
			rf.logger.Println(me, "Candidate RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)

			reply := &AppendEntriesReply{}
			if args.Term < term {
				// Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply
			} else {
				HandleAppendEntry(me, term, &args, reply, rf)

				go rf.followerRoutine(args.Term, args.Leader_id) // convert to follower
				return
			}

			// ticker = time.NewTicker(getRandomTime())

		// case <- time.After(getRandomTime()):
		case <-ticker.C:
			// Election Timed Out //
			// rf.logger.Println(me, "C timeout", term)
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
			rf.logger.Println(me, "C timeout", term)
			candidateRequestVotes(requestVote_rpcResult_chan, term, me, rf)


		case reply := <- requestVote_rpcResult_chan:
			// rf.logger.Println("In Candidate requestVoteRPC response", term, reply.Term, reply.Vote_granted, reply.Voted_for)

			// Check reply & Process results

			if reply.Term > term {
				// convert to follower
				// rf.logger.Println(me, "C vote failed greater term, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
				go rf.followerRoutine(reply.Term, NULL_INT) // Set as NULL_INT not leader for now
				return
			} else if reply.Term == term {
				if reply.Vote_granted == true {
					num_votes_granted += 1
					// rf.logger.Println(me, "C vote success, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
					if num_votes_granted > len(rf.peers) / 2 {
						// Election Succeeded
						// rf.logger.Println(me, "Election Success", num_votes_granted, len(rf.peers))
						go rf.leaderRoutine(term)
						return
					}
				}
				// rf.logger.Println(me, "C vote failed firstcomefirstserve, currentTerm:", term, "reply term:", reply.Term, "success:", reply.Vote_granted, "F/C voted_for:", reply.Voted_for, "Vote Count:", num_votes_granted, "num_peers:", len(rf.peers))
			} else {
				rf.logger.Panicln("SOMETHING WEIRD HERE: requestVote rpc reply.Term < term", term, reply.Term)
			}

		case <- rf.close_mainRoutine_chan:
			return
		}
	}
}

// func sendHeartBeat(appendEntries_rpcResult_chan chan AppendEntriesReply, term int, me int, rf *Raft) {
// 	// rf.logger.Println(me, "Leader sending Heartbeats, term:", term)
// 	for i:=0; i < len(rf.peers); i++ {
// 		if i != me {
// 			args := &AppendEntriesArgs{}
// 			args.Leader_id = me
// 			args.Term = term
// 			args.Log_entries = nil

// 			// rf.logger.Println(me, "sending HeartBeat to ", i)
// 			go func(peer_id int, args AppendEntriesArgs, appendEntries_rpcResult_chan chan AppendEntriesReply) {
// 				reply := &AppendEntriesReply{}
// 				status := rf.sendAppendEntries(peer_id, &args, reply)
// 				if status == true {
// 					appendEntries_rpcResult_chan <- *reply
// 				}
// 				return
// 			}(i, *args, appendEntries_rpcResult_chan)
// 		}
// 	}

// 	return
// }

func sendAppendEntriesAll(appendEntries_rpcResult_chan chan AppendEntriesReply, nextIndices []int, term int, me int, rf *Raft) {
	// rf.logger.Println(me, "Leader sending Heartbeats, term:", term)
	last_log_id := get_last_log_id(rf)

	for i:=0; i < len(rf.peers); i++ {
		if i != me {
			// rf.logger.Println(me, "sending AppendEntry to ", i)
			next_index_i := nextIndices[i]
			args := &AppendEntriesArgs{}
			args.Leader_id = me
			args.Term = term

			if last_log_id >= next_index_i {
				// Send Append LogEntry //
				rf.mux.Lock()
				rf.logger.Println("Next_index_i", next_index_i, "i:", i)
				if next_index_i == 1 {
					args.Prev_log_index = NULL_INT // NOTE: Receiver should check if log_index == NULL_INT -> always passes consistentCheck
					args.Prev_log_term = NULL_INT
				} else {
					args.Prev_log_index = next_index_i - 1
					args.Prev_log_term = rf.log_entries[args.Prev_log_index - 1].Term // minus 1 because of 1-indexing
				}

				/* Init Log Entries to send */
				// args.Log_entries = make([]LogEntry, len(rf.log_entries[next_index_i - 1:]))
				// copy(args.Log_entries, rf.log_entries[next_index_i - 1:]) // minus 1 to conver to 0-index
				// NEW //
				args.Log_entries = make([]LogEntry, 0)
				args.Log_entries = rf.log_entries[next_index_i - 1:] // minus 1 to conver to 0-index
				// for j := next_index_i - 1; j < rf.num_logs; j++ {
				// 	args.Log_entries[j - next_index_i + 1] = rf.log_entries[j]
				// }
				// args.Log_entries_len = rf.num_logs - next_index_i + 1
				args.Is_heartbeat = false //


				rf.mux.Unlock()
				args.Leader_commit_id = rf.commit_index

			} else {
				// Send HeartBeat //
				// rf.logger.Println(me, "sending HeartBeat to ", i)
				args.Is_heartbeat = true
			}

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

func (rf *Raft) leaderRoutine(term int) {
	rf.mux.Lock()
	rf.current_term = term
	rf.logger.Println(rf.me, "just became leader term:", term)
	// printLogs(rf.me, rf.log_entries)
	rf.server_state = 3

	me := rf.me
	rf.mux.Unlock()

	var nextIndices []int
	var matchIndices []int

	rf.logger.Println(rf.me, "term:", term, "here0")
	last_log_id := get_last_log_id(rf)
	rf.logger.Println(rf.me, "term:", term, "here1")

	for i:=0; i < len(rf.peers); i++ {
		if last_log_id == NULL_INT {
			nextIndices = append(nextIndices, 1)
		} else {
			nextIndices = append(nextIndices, last_log_id + 1)
		}
		matchIndices = append(matchIndices, 0)
	}


	rf.logger.Println(rf.me, "term:", term, "here2")
	appendEntries_rpcResult_chan := make(chan AppendEntriesReply)
	rf.logger.Println(rf.me, "term:", term, "here3")

	// Send Heartbeats /* Send AppendEntries RPC in a new go routine */
	sendAppendEntriesAll(appendEntries_rpcResult_chan, nextIndices, term, me, rf)
	// sendHeartBeat(appendEntries_rpcResult_chan, term, me, rf)
	rf.logger.Println(rf.me, "term:", term, "here4")

	var ticker *time.Ticker
	ticker = time.NewTicker(time.Duration(heartbeat_interval) * time.Millisecond)

	for {
		select {
		case args := <- rf.received_requestVote_chan:
			rf.logger.Println(rf.me, "C RequestVote received, term:", args.Term, args.Candidate_id)
			reply := &RequestVoteReply{}

			if args.Term < term {
				// reject -> another more recent Leader is present
				reply.Term = term
				reply.Vote_granted = false
				reply.Voted_for = 10000 // Signify I am the leader already
				rf.requestVote_reply_chan <- *reply
				rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "L falied-lowerTerm, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

			} else if args.Term == term {
				// NOTE: Break Ties by comparing log up-to-date ness
				reply.Term = args.Term
				if get_upToDate_check(args, rf) == true {
					reply.Vote_granted = true
					rf.requestVote_reply_chan <- *reply

					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "L success-EqualTerm-UpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

				} else {
					reply.Vote_granted = false
					reply.Voted_for = 10000 // Signify I am the leader already
					rf.requestVote_reply_chan <- *reply

					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "L failed-EqualTerm-NotUpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

					go rf.followerRoutine(args.Term, args.Candidate_id) // convert to follower
					return
				}
				// rf.logger.Println(me, "L RequestVote reply to Candidate:", args.Candidate_id, "voted_for:", reply.Voted_for, "args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)

			} else {
				// args.Term > term
				// TODO check if candidate’s log is at least as up-to-date as receiver's log
				reply.Term = args.Term
				if get_upToDate_check(args, rf) {
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "L success-HigherTerm-UpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					reply.Vote_granted = true
				} else {
					rf.logger.Println(args.Candidate_id, "RequestVote reply from", me, "L failed-HigherTerm-NotUpToDate, args term", args.Term, "current_term:", term, "vote granted:", reply.Vote_granted)
					reply.Vote_granted = false
				}
				rf.requestVote_reply_chan <- *reply

				// rf.logger.Println(me, "lost leadership to candidate:", args.Candidate_id,"current_term:", term, "candidate term", args.Term)
				go rf.followerRoutine(args.Term, args.Candidate_id) // convert to follower
				return
			}

		case args := <- rf.received_appendEntries_chan:
			rf.logger.Println(me, "LEADER RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)

			reply := &AppendEntriesReply{}
			if args.Term < term {
				// Sender Leader is outdated
				reply.Success = false
				reply.Term = term
				rf.appendEntries_reply_chan <- *reply
			} else {
				HandleAppendEntry(me, term, &args, reply, rf)

				if args.Term > term {
					// go rf.followerRoutine(args.Term, args.Leader_id) // convert to follower
					// rf.logger.Println(me, "lost leadership to another leader:", args.Leader_id,"current_term:", term, "candidate term", args.Term)
					go rf.followerRoutine(args.Term, NULL_INT) // convert to follower
					return

				} else if args.Term == term {
					if args.Leader_id != me {
						rf.logger.Println("2 Leaders in the same term -> have not decided how to handle", me,  args.Leader_id)
						rf.logger.Panicln(me, "LEADER RECEIVED HEARTBEAT from:", args.Leader_id, "args.term:", args.Term, "currentTerm:", term)
						// Case should never happen -> Handle if it does -> Consider comparing which leader is more up-to-date
						// rf.logger.Println("2 Leaders in the same term -> have not decided how to handle", me,  args.Leader_id)
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
			// rf.logger.Println(me, "L Heartbeat resp, term:", reply.Term, "success:", reply.Success)
			rf.logger.Println("leader reply", reply.From, "matchid", reply.New_match_id, "is heartbeat", reply.Is_heartbeat)
			if reply.Success == false {
				if reply.Term > term {
					// Leader is outdated -> update term and switch to Follower
					// rf.logger.Println(me, "lost leadership during heartbeat response, current_term:", term, "candidate term", reply.Term)
					go rf.followerRoutine(reply.Term, NULL_INT)
					return
				} else {
					if reply.Is_heartbeat == false {
						// Case where log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
					rf.logger.Println(rf.me, "L received AppendEntries responds from", reply.From, "failed logInconsistent", term, reply.Term, reply.Success, reply.New_match_id)

					// TODO: decrement nextIndex and retry
					// decrement nextIndices[i]
					nextIndices[reply.From] = nextIndices[reply.From] - 1
					next_index_i := nextIndices[reply.From] // 1-indexed

					// Retry
					args := &AppendEntriesArgs{}
					args.Leader_id = me
					args.Term = term

					// Make sure next_index_i <= last_log_id
					if next_index_i == 1 {
						args.Prev_log_index = NULL_INT // NOTE: Receiver should check if log_index == NULL_INT -> always passes consistentCheck
						args.Prev_log_term = NULL_INT
					} else {
						args.Prev_log_index = next_index_i - 1
						rf.mux.Lock()
						args.Prev_log_term = rf.log_entries[args.Prev_log_index - 1].Term // minus 1 because of 1-indexing
						rf.mux.Unlock()
					}

					rf.mux.Lock()
					/* Init Log Entries to send */
					args.Log_entries = make([]LogEntry, len(rf.log_entries[next_index_i - 1:]))
					copy(args.Log_entries, rf.log_entries[next_index_i - 1:]) // minus 1 to conver to 0-index
					// for j := next_index_i - 1; j < rf.num_logs; j++ {
					// 	args.Log_entries[j - next_index_i - 1] = rf.log_entries[j]
					// }
					// args.Log_entries_len = rf.num_logs - next_index_i + 1
					// args.Is_heartbeat = false //
					rf.mux.Unlock()
					args.Leader_commit_id = rf.commit_index

					go func(peer_id int, args AppendEntriesArgs, appendEntries_rpcResult_chan chan AppendEntriesReply) {
						reply := &AppendEntriesReply{}
						status := rf.sendAppendEntries(peer_id, &args, reply)
						if status == true {
							appendEntries_rpcResult_chan <- *reply
						}
						return
					}(reply.From, *args, appendEntries_rpcResult_chan) //
				}
			}

			} else {

				if reply.Is_heartbeat == false {
					rf.logger.Println(rf.me, "L received AppendEntries non-HB response from", reply.From, "Success", term, reply.Term, "match_id", reply.New_match_id)
					/*
					1. Update nextIndex and matchIndex for follower
					2. Update Commit Index
						If there exists an N such that N > commitIndex, a majority
						of matchIndex[i] ≥ N, and log[N].term == currentTerm:
						set commitIndex = N
					3. if commit index > last applied -> Apply and send to client
					*/

					// 1. Update nextIndex and matchIndex for follower
					nextIndices[reply.From] = reply.New_match_id + 1 // To ASK
					matchIndices[reply.From] = reply.New_match_id // To ASK

					last_log_id := get_last_log_id(rf)

					for log_id := rf.commit_index + 1; log_id <= last_log_id; log_id++ {
						num_peers_commited_i := 0
						// Check if a majority of matchIndex[i] ≥ N
						for j := 0; j < len(rf.peers); j++ {
							if matchIndices[j] >= log_id {
								num_peers_commited_i += 1
							}
						}
						rf.logger.Println("matchIndex for log", log_id, "num peer commited", num_peers_commited_i)
						if num_peers_commited_i > ((len(rf.peers) / 2) - 1) { // minus 1 because we aren't counting leader himself
							// Check if log[N].term == currentTerm
							rf.mux.Lock()
							log_i := rf.log_entries[log_id - 1] // minus 1 for 1-indexing
							rf.mux.Unlock()
							if log_i.Term == term {
								apply_command := &ApplyCommand{log_i.Index, log_i.Command}
								rf.applyCh <- *apply_command

								rf.commit_index = log_id
								rf.last_applied = log_id
							}
						}
					}
				}
			}

		// case <- time.After(time.Duration(heartbeat_interval) * time.Millisecond):
		case <-ticker.C:
			// Send AppendEntries
			sendAppendEntriesAll(appendEntries_rpcResult_chan, nextIndices, term, me, rf)
			// sendHeartBeat(appendEntries_rpcResult_chan, term, me, rf)
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

	// rf.logger.Println("GET STATE RESULTS:", me, current_term, isLeader)

	return me, current_term, isLeader
}


type LogEntry struct {
	Index 	int  // might need this if we need 1-indexing
	Term		int
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
	Term				int
	Leader_id			int

	Prev_log_index		int
	Prev_log_term		int

	// Log_entries			[MAX_ARG_LOG_ENTRIES]LogEntry
	Log_entries			[]LogEntry
	// Log_entries			LogEntry
	// Log_entries_len		int
	Is_heartbeat		bool

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
	From			int  // server index providing reply

	New_match_id	int	// 1-indexed, NULL_INT if not applicable
	Is_heartbeat	bool
	//
}


// AppendEntries()
// ===============
// Example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
	1. Reply false if term < currentTerm
	2.
	*/
	// rf.mux.Lock()
	// me := rf.me
	// rf.mux.Unlock()

	// rf.logger.Println(me, "In APpendEntires handler log_len", len(args.Log_entries))
	// printLogs(me, (*args).Log_entries[:], len(args.Log_entries))
	rf.received_appendEntries_chan <- *args
	server_reply := <- rf.appendEntries_reply_chan

	reply.Term = server_reply.Term
	reply.Success = server_reply.Success
	reply.From = server_reply.From
	reply.New_match_id = server_reply.New_match_id
	reply.Is_heartbeat = server_reply.Is_heartbeat
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.logger.Println("L printing logs to send to ", server, "arg log len:", len(args.Log_entries))
	// printLogs(1, args.Log_entries[:], len(args.Log_entries)) // Still okay here
	// rf.logger.Println(me, "L printing logs to send to ", i)
	// printLogs(me , args.Log_entries[:], args.Log_entries_len)
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
	// rf.logger.Println(rf.me, "RequestVoteRPC handler called", "from:", args.Candidate_id, "term:", args.Term)
	rf.received_requestVote_chan <- *args
	// rf.logger.Println(rf.me, "RV RPC 2")
	server_reply := <-rf.requestVote_reply_chan

	reply.Term = server_reply.Term
	reply.Vote_granted = server_reply.Vote_granted
	reply.Voted_for = server_reply.Voted_for

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

		new_log := LogEntry{new_log_id, current_term, command}

		rf.log_entries = append(rf.log_entries, new_log)
		rf.num_logs += 1

		rf.logger.Println(rf.me, "L receiving PutCommand term:", current_term, "index",  new_log_id, "command", command)
		// printLogs(rf.me, rf.log_entries)
		rf.mux.Unlock()
		return new_log_id, current_term, true
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
		// logPrefix := fmt.Sprintf("%s ", peerName)
		logPrefix := fmt.Sprintf("%d_server", me)
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
