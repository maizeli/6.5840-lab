package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/util"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	ElectionTimeout   int32        // 选举超时时间 单位/ms
	Term              int          // 任期
	VoteIdx           int          // 投票的server
	ServerStatus      ServerStatus // 当前状态
	HeartbeatInterval int          // 每秒发送的心跳包
	LastHeartbeatTime int64        // 上次心跳时间 单位:ms
	// 2B
	Logs         []*Log
	NextIdxs     []int32
	MatchIdxs    []int32
	ApplyMsgCh   chan ApplyMsg
	CommittedIdx int        // 已经提交的Idx
	StartMu      sync.Mutex // 用于保证Start的串行性
}

func (rf *Raft) findTermFirstCommand(term int) *Log {
	var res *Log
	for i := len(rf.Logs) - 1; i >= 0; i-- {
		if rf.Logs[i] == nil {
			util.Logger.Printf("findTermFirstCommand [S%v] rf.Logs[%v]=nil", rf.me, i)
			continue
		}
		if rf.Logs[i].Term == term {
			res = rf.Logs[i]
		}
		if rf.Logs[i].Term < term {
			break
		}
	}

	return res
}

type Log struct {
	Term    int         // 当前命令所属Term
	Idx     int         // 当前命令所属Index
	Command interface{} // 命令内容
}

type ServerStatus int32

const (
	ServerStatusFollower  ServerStatus = 1
	ServerStatusLeader    ServerStatus = 2
	ServerStatusCandidate ServerStatus = 3
)

func (s ServerStatus) String() string {
	switch s {
	case ServerStatusCandidate:
		return "Candidate"
	case ServerStatusLeader:
		return "Leader"
	case ServerStatusFollower:
		return "Follower"
	default:
		return "None"
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = int(rf.Term)
	isleader = rf.ServerStatus == ServerStatusLeader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

// 当前server当选leader之后需要做的初始化工作
// 1. 重置NextIdxMap TODO 这里是否使用CommittedIdx
func (rf *Raft) LeaderInit() {
	util.Logger.Printf("[LeaderInit] [S%v] [T%v] start...", rf.me, rf.Term)
	rf.MatchIdxs, rf.NextIdxs = make([]int32, len(rf.peers)), make([]int32, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIdxs[i] = int32(len(rf.Logs))
		rf.MatchIdxs[i] = -1
	}
	util.Logger.Printf("[LeaderInit] [S%v] [T%v] end...", rf.me, rf.Term)
}

/*
持久化数据分析
0. ServerStatus 		NO
	Leader挂掉之后应当从Server开始
1. Term					YES
2. VoteIdx				YES
3. Logs					YES

4. ElectionTimeout		NO
5. HeartbeatInterval	NO
6. LastHeartbeatTime	NO
7. StartMu				NO
8. ApplyMsgCh			NO
上面这四个一定是不需要持久化的

8. NextIdxs				NO
9. matchIdxs			NO
这两个在每次当选leader的时候都要重置，所以没必要持久化

10. CommittedIdx 		NO
这个需要分析下用处
	a. 当前server为Leader时，Start需要CommittedIdx决定当前命令的Idx
		如果当前server崩溃时间较长，剩余server选举出新leader，那么此时Start也就用不到CommittedIdx了
		如果当前server崩溃时间较短，恢复之后其余大部分server心跳未超时时，如果ServerStatus被持久化时，此值也需要被持久化，如果ServerStatus没有被持久化，此值也不需要被持久化
	b. 提交Log
		Leader提交，此时不需要
		Server提交，此时使用的时Leader的CommittedIdx也不需要
	c.
*/

// PersistState 需持久化的信息
type PersistState struct {
	Term         int
	VoteIdx      int
	Logs         []*Log
	CommittedIdx int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	persistState := PersistState{
		Term:         rf.Term,
		VoteIdx:      rf.VoteIdx,
		Logs:         rf.Logs,
		CommittedIdx: rf.CommittedIdx,
	}
	encoder.Encode(persistState)
	raftState := buf.Bytes()
	rf.persister.Save(raftState, nil)
	util.Logger.Printf("[persist] [S%v] success:%v", rf.me, util.JSONMarshal(persistState))

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	persistState := PersistState{}
	if err := decoder.Decode(&persistState); err != nil {
		util.Logger.Printf("[readPersist] [S%v] decode fail, err=%v", rf.me, err)
		return
	}
	rf.Term = persistState.Term
	rf.VoteIdx = persistState.VoteIdx
	rf.Logs = persistState.Logs
	rf.CommittedIdx = persistState.CommittedIdx
	util.Logger.Printf("[readPersist] [S%v] success: %v", rf.me, util.JSONMarshal(persistState))

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 选举人参加选举的任期
	ServerIdx   int // 选举人的下标
	LastLogIdx  int // 最后一条log的下标
	LastLogTerm int // 最后一条log的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	ServerIdx int  // 跟随者的server下标
	Term      int  // 当前Follower的任期
	IsVote    bool // 是否响应此次选举 是否有效有待考量
	// For Debug
	DenyReason string // 拒绝投票原因
}

// example RequestVote RPC handler.
/*
考虑接收到此请求时，Server的状态
1. Candidate
a. Term > 当前 是否投票
b. Term = 当前
c. Term < 当前 NO
2. Follower
a. Term > 当前
b. Term = 当前
c. Term < 当前 NO
3. Leader
a. Term > 当前
b. Term = 当前
c. Term < 当前 NO

2B增加判断
在2A的基础上判断args.LastLogTerm以及args.LastLogIndex是否>=当前server的
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	util.Logger.Printf("[RequestVote] [S%v] [T%v] recv [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, args.Term)
	defer func() {
		if reply.IsVote {
			util.Logger.Printf("[RequestVote] [S%v] [T%v] vote for [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, rf.Term)
		} else {
			util.Logger.Printf("[RequestVote] [S%v] [T%v] deny [S%v] [T%v] RequestVote:%v", rf.me, rf.Term, args.ServerIdx, rf.me, reply.DenyReason)
		}
	}()
	reply.Term = rf.Term
	reply.ServerIdx = rf.me
	// 1. 过期Term
	if args.Term < rf.Term {
		reply.DenyReason = fmt.Sprintf("term less")
		return
	}

	/*
		Term大于当前Server Term时,无论其Log是否满足选择Leader的条件,当前Server都应该先转换状态为Follower
		此时不需要更新上次心跳时间,以便当前server也可以发起选举:防止当前Server是唯一一个能当选的的Server,由于被其他Server频繁的"打扰",无法发起选举
	*/
	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.changeStatus(ServerStatusFollower, -1, false)
		// Last Log比较
		lastLogTerm, lastLogIdx := 0, 0
		if len(rf.Logs) > 0 {
			lastLog := rf.Logs[len(rf.Logs)-1]
			lastLogTerm = lastLog.Term
			lastLogIdx = lastLog.Idx
		}
		if args.LastLogTerm < lastLogTerm {
			reply.DenyReason = fmt.Sprintf("last log term less: [T%v] < [T%v]", args.LastLogTerm, lastLogTerm)
			return
		}
		if args.LastLogTerm == lastLogTerm && args.LastLogIdx < lastLogIdx {
			reply.DenyReason = fmt.Sprintf("last log idx less: [I%v] < [I%v]", args.LastLogIdx, lastLogIdx)
			return
		}
		reply.IsVote = true
		rf.changeStatus(ServerStatusFollower, args.ServerIdx, reply.IsVote)
		return
	}

	if args.Term == rf.Term {
		if rf.VoteIdx == -1 || rf.VoteIdx == args.ServerIdx {
			reply.IsVote = true
			rf.changeStatus(ServerStatusFollower, args.ServerIdx)
		} else {
			reply.DenyReason = fmt.Sprintf("already vote for %v", rf.VoteIdx)
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// Start只能是串行的,并发提交两个命令可能会使用一个Index
// 如果大多数server都复制了此消息，但是此时Server已经不再是leader怎么办？
// Start只是添加Log,实际执行复制动作的是HearBeat

/*
2CFigure8失败原因
5个server
第一轮S1为leader，5个server 在 idx 0 成功commit command0
第二轮S1继续leader 5个server在 idx 1 均添加好command1，此时S1提交之后挂掉了，其余4个没来得及提交
第三轮S2当选Leader，2 3 4 5 在 idx1 添加好 command2，2开始提交，此时发现S1提交的跟S2有冲突

如何解决？
目前看是commit的时机不对

上述分析不对，不是commit的时机不对，而是Start获取当前Log应该放到最新的一条Log，而不是已经Commit的LogIdx+1
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 加锁保证只有一个Start在运行
	rf.StartMu.Lock()
	defer rf.StartMu.Unlock()

	idx := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	// 开始前更新CommittedIdx，防止刚从crassh中恢复，CommittedIdx未恢复
	rf.UpdateCommitedIdx()
	util.Logger.Printf("[Start] [S%v] [T%v] command = %v, logs=%v, CommittedIdx=%v", rf.me, rf.Term, util.JSONMarshal(command), util.JSONMarshal(rf.Logs), rf.CommittedIdx)
	isLeader = rf.ServerStatus == ServerStatusLeader
	// 非Leader直接返回
	if !isLeader {
		util.Logger.Printf("[Start] [S%v] [T%v] is not leader", rf.me, rf.Term)
		rf.mu.Unlock()
		return idx, term, isLeader
	}
	// 当前命令Idx=CommittedIdx+1
	committedIdx := rf.CommittedIdx
	idx = len(rf.Logs)
	// 记录当前Term
	term = rf.Term
	// 将command添加到leader本地状态机
	log := &Log{
		Term:    term,
		Idx:     idx,
		Command: command,
	}
	rf.Logs = append(rf.Logs, log)
	rf.mu.Unlock()

	wg, replyMu := &sync.WaitGroup{}, sync.Mutex{}
	replys := make([]*AppendEntriesReply, len(rf.peers))
	// Leader开始复制新Log
	cnt := int32(0)
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		i, peer := i, peer

		wg.Add(1)
		go func() {
			defer wg.Done()
			// 死循环复制
			for !rf.killed() {
				rf.mu.Lock()
				// 获取对应Follower从哪一条Log开始复制
				prevLogIdx, prevLogTerm := atomic.LoadInt32(&rf.NextIdxs[i])-1, -1
				// 用于后面更新NextIdxs/MatchIdxs
				logLen := len(rf.Logs)
				if len(rf.Logs) > 0 && prevLogIdx >= 0 {
					prevLogTerm = rf.Logs[prevLogIdx].Term
				}
				args := &AppendEntriesArgs{
					Term:            term,
					LeaderIdx:       rf.me,
					PrevLogIdx:      int(prevLogIdx),
					PrevLogTerm:     prevLogTerm,
					LeaderCommitIdx: committedIdx,
				}
				if prevLogIdx+1 >= 0 {
					args.Logs = rf.Logs[prevLogIdx+1:]
				}
				rf.mu.Unlock()

				util.Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] args=%v", rf.me, term, i, util.JSONMarshal(args))

				reply := &AppendEntriesReply{}
				res := peer.Call("Raft.AppendEntries", args, reply)
				if !res {
					util.Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v], res=false", rf.me, term, i)
					return
				}
				replyMu.Lock()
				replys[i] = reply
				replyMu.Unlock()
				rf.mu.Lock()
				// 发起Start时的任期已过期,跳出循环
				if term != rf.Term {
					util.Logger.Printf("[Start] [S%v] [T%v] send Command[%v], recv [S%v]'s response on [T%v]", rf.me, term, command, i, rf.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				// 如果出现并发更新可能会导致NextIdxs/MatchIdxs回拨?影响可控
				if reply.Success {
					rf.mu.Lock()
					atomic.AddInt32(&cnt, 1)
					atomic.StoreInt32(&rf.NextIdxs[i], int32(logLen))
					atomic.StoreInt32(&rf.MatchIdxs[i], int32(logLen)-1)
					util.Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] success, NextIdx[%v]->%v, MatchIdx[%v]->%v", rf.me, term, i, i, logLen, i, logLen-1)
					rf.mu.Unlock()
					break
				} else {
					// Term小于,直接返回
					if reply.Term > term {
						return
					}
					// 因为前一条Log不匹配，NextLogIdx-1，再试下
					util.Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] fail, log idx -1 and retry", rf.me, term, i)
					if prevLogIdx <= -1 {
						util.Logger.Printf("[Start] [S%v] [T%v] prev log idx less than -1, break", rf.me, term)
						break
					}
					atomic.StoreInt32(&rf.NextIdxs[i], prevLogIdx)
				}
			}
		}()
	}

	timeOut := util.WaitWithTimeout(wg, time.Duration(rf.ElectionTimeout)*time.Millisecond, nil)
	if timeOut {
		util.Logger.Printf("[Start] [S%v] [T%v] time out", rf.me, term)
	} else {
		util.Logger.Printf("[Start] [S%v] [T%v] start successful", rf.me, term)
	}
	// 如果返回的Term存在大于当前Term的要及时更新当前Term
	maxTerm := term
	replyMu.Lock()
	for _, reply := range replys {
		if reply == nil {
			continue
		}
		maxTerm = util.Max(maxTerm, reply.Term)
	}
	replyMu.Unlock()
	if maxTerm > term {
		rf.mu.Lock()
		if maxTerm > rf.Term {
			rf.changeStatus(ServerStatusFollower, -1)
			rf.Term = maxTerm
		}
		util.Logger.Printf("[Start] [S%v] term is timeout [T%v] < [T%v]", rf.me, term, rf.Term)
		rf.mu.Unlock()
		return idx, maxTerm, false // 这里+1是因为测试认为idx从1开始
	}
	if int(atomic.LoadInt32(&cnt)) < len(rf.peers)/2 {
		util.Logger.Printf("[Start] [S%v] [T%v] less than half server aggree command, cnt(%v) %v", rf.me, term, cnt, command)
	} else {
		util.Logger.Printf("[Start] [S%v] [T%v] more than half server aggree command cnt(%v) %v", rf.me, term, cnt, command)
		// 不能在这里Commit，不然会出现Figure8中的问题，只能在Start前通过MatchIdx数组进行Commit
		// rf.mu.Lock()
		// rf.CommitIdx(idx)
		// rf.mu.Unlock()
	}

	return idx + 1, term, true
}

// CommitIdx 执行CommitIdx需在外层获取rf.mu
func (rf *Raft) CommitIdx(idx int) {
	util.Logger.Printf("[CommitIdx] [S%v] commit %v", rf.me, idx)
	if idx <= rf.CommittedIdx {
		util.Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v <= rf.CommitedIdx %v, not need commit", rf.me, rf.Term, idx, rf.CommittedIdx)
		return
	}
	if idx >= len(rf.Logs) {
		util.Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v > len(rf.Logs) %v, not need commit", rf.me, rf.Term, idx, len(rf.Logs))
		return
	}
	for i := rf.CommittedIdx + 1; i <= idx; i++ {
		rf.ApplyMsgCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[i].Command,
			CommandIndex: i + 1,
		}
	}
	rf.CommittedIdx = idx
	// 持久化
	rf.persist()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	util.Logger.Printf("[Kill] [S%v] is killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		now := time.Now()
		rf.mu.Lock()
		lastHeartbeatTime := rf.LastHeartbeatTime
		rf.mu.Unlock()
		// 心跳超时发起选举
		if now.UnixMilli()-lastHeartbeatTime >= int64(rf.ElectionTimeout) {
			rf.StartElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	util.Logger.Printf("[ticker] [S%v] stop listen heart beat timeout", rf.me)
}

/*
发起选举
最长时间为心跳超时时间
*/
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	if rf.ServerStatus == ServerStatusLeader {
		rf.mu.Unlock()
		return
	}
	// 拿到锁之后再次验证状态
	// 中间有可能收到其他leader发来的心跳
	now := time.Now()
	if now.UnixMilli()-rf.LastHeartbeatTime < int64(rf.ElectionTimeout) {
		rf.mu.Unlock()
		return
	}
	util.Logger.Printf("[Election] [S%v] [T%v] start election", rf.me, rf.Term)
	// 1. 给自己投一票 这里应该不会失败
	res := rf.changeStatus(ServerStatusCandidate, rf.me)
	if !res {
		rf.mu.Unlock()
		return
	}
	rf.Term += 1
	localTerm := rf.Term
	var lastLog *Log
	if len(rf.Logs) > 0 {
		lastLog = rf.Logs[len(rf.Logs)-1]
	}
	rf.mu.Unlock()

	// 2. 向其他服务器发起投票
	wg := sync.WaitGroup{}
	replyChan := make(chan *RequestVoteReply, len(rf.peers))
	replys := []*RequestVoteReply{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:      localTerm,
				ServerIdx: rf.me,
			}
			if lastLog != nil {
				args.LastLogIdx = lastLog.Idx
				args.LastLogTerm = lastLog.Term
			}
			reply := &RequestVoteReply{}
			util.Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote...", rf.me, localTerm, i)
			res := rf.sendRequestVote(i, args, reply)
			if !res {
				util.Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote fail", rf.me, localTerm, i)
				return
			}
			replyChan <- reply
		}()
	}
	// 这里不能傻傻等待所有服务器响应，只要超过一半的server投票，就应该停止
	voteCount, flag, tCh := 0, false, time.After(time.Millisecond*time.Duration(rf.ElectionTimeout)*1/2)
	for {
		// 超时或者投票数量超过一般立即退出
		if flag {
			break
		}
		select {
		case reply := <-replyChan:
			if reply != nil && reply.IsVote {
				voteCount += 1
			}
			if voteCount >= len(rf.peers)/2 {
				flag = true
			}
			replys = append(replys, reply)
		case <-tCh:
			flag = true
			// 这里要添加default语句是的flag可以快速被执行到 这里添加了default反而收到投票之后没有跳出
		default:
		}
	}
	// 判断是否存在ServerTerm大于当前
	var maxTerm int
	for _, reply := range replys {
		if reply == nil {
			util.Logger.Printf("[Election] [S%v] invalid reply", rf.me)
			continue
		}
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		if reply.IsVote {
			util.Logger.Printf("[Election] [S%v] receive vote from [S%v]", rf.me, reply.ServerIdx)
		} else {
			util.Logger.Printf("[Election] [S%v] NOT receive vote from [S%v]", rf.me, reply.ServerIdx)
		}
	}
	// 统计结果
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 此时term已经过期 已经变成其他人的小弟or已经开始下一轮选举
	if rf.Term != localTerm {
		util.Logger.Printf("[Election] [S%v] [T%v] term is not equal, current term is [%v]", rf.me, rf.Term, localTerm)
		return
	}
	// 改变当前状态
	if maxTerm > rf.Term {
		// 这里不应该更新心跳时间？好发起下一次
		rf.changeStatus(ServerStatusFollower, -1, false)
		return
	}
	// 投票过程已经重新变为 Follower
	if rf.ServerStatus == ServerStatusFollower {
		util.Logger.Printf("[Election] [S%v] [T%v] already become a follower", rf.me, rf.Term)
		return
	}
	// 包含等于:因为自己会给自己投一票
	if voteCount >= len(rf.peers)/2 {
		util.Logger.Printf("[Election] [S%v] [T%v] receive major vote[%v]", rf.me, rf.Term, voteCount)
		_ = rf.changeStatus(ServerStatusLeader, rf.me)
	} else {
		util.Logger.Printf("[Election] [S%v] [T%v] not receive enough vote", rf.me, rf.Term)
	}
}

/*
Follower->Candidate: 一定时间未收到Leader心跳

Candidate->Leader: 获得大多数选票
Candidate->Follower: 选举时收到其他Leader的心跳
Candidate->Candidate: 选举时未得到大多数的投票,开启下一轮

Leader->Follwer: 收到大于当前Term中Leader的心跳
Leader->Candidate: leader未收到大多数的心跳时,应该变为Candidate
Leader->Leader: Leader续期
*/
var serverStatusCheck = map[ServerStatus][]ServerStatus{
	ServerStatusCandidate: {
		ServerStatusLeader,
		ServerStatusFollower,
		ServerStatusCandidate,
	},
	ServerStatusFollower: {
		ServerStatusCandidate,
		ServerStatusFollower, // 变成更高任期的Follower
	},
	ServerStatusLeader: {
		ServerStatusFollower,
		ServerStatusCandidate,
		ServerStatusLeader, // 用于心跳更新任期
	},
}

// changeStatus 改变当前server的状态，执行此函数需在外层枷锁
// 更新Server状态，并且更新投票信息以及心跳信息
/*
toStatus
1. Follower serverIdx为新任 leader idx
2. Candidate serverIdx为自己
3. Leader serverIdx为自己
*/
func (rf *Raft) changeStatus(toStatus ServerStatus, serverIdx int, updateHeartBeats ...bool) bool {
	flag := false
	for _, status := range serverStatusCheck[rf.ServerStatus] {
		if status == toStatus {
			flag = true
		}
	}
	if !flag {
		util.Logger.Printf("[changeStatus] [S%v] change server status fail [%v]->[%v]", rf.me, rf.ServerStatus, toStatus)
		return false
	}
	util.Logger.Printf("[changeStatus] [S%v] change server status [%v]->[%v]", rf.me, rf.ServerStatus, toStatus)
	oldStatus := rf.ServerStatus
	rf.ServerStatus = toStatus
	// 这里更新LastHeartBeattime
	// 如果toStatus是Follower，更新是因为收到了leader的心跳
	// 如果toStatus是Candidate, 更新是防止重复发起选举
	// 如果toStatus是Leader, 更新是续约
	if len(updateHeartBeats) == 0 || updateHeartBeats[0] {
		rf.LastHeartbeatTime = time.Now().UnixMilli()
	}
	if toStatus == ServerStatusLeader {
		rf.VoteIdx = serverIdx
		// 只有之前不是Leader再执行即可
		if oldStatus != ServerStatusLeader {
			rf.LeaderInit()
		}
	}
	if toStatus == ServerStatusFollower {
		rf.VoteIdx = serverIdx
	}
	if toStatus == ServerStatusCandidate {
		rf.VoteIdx = serverIdx
	}
	// 持久化
	rf.persist()

	return true
}

// AppendEntries
/*
1. 判断Args.Term Index
3个server，T1时刻 S1是leader，S2,S3是follower
S3网络出现问题，无法发送也无法接受消息。
S3开始发起选举，由于无法发送消息，其只能不断增加自身的Term，并处于candidate状态
期间S1和S2正常通信，S1和S2的Term一直不变，并接受了command
S3网络恢复，由于此时S3的Term比较大，S1 -> S3的心跳被S3拒绝，由于S3的缺少部分commited command，其也不能当选leader

如何让S3接受S1的心跳？
这里不能让S3接受S1的心跳，而是应该让S1的Term跟上S3的Term，并重新当选server


*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// util.Logger.Printf("[AppendEntries] [S%v] [T%v] recv [S%v] [T%v]", rf.me, rf.Term, args.LeaderIdx, args.Term)
	const (
		AppendEntriesResultSuccess                 = 1
		AppendEntriesResultDenyTermLess            = 2
		AppendEntriesResultDenyTermEqualDiffLeader = 3
		AppendEntriesResultDenyTermEqualNowLeader  = 4
		AppendEntriesResultDenyInvalidPrevLog      = 5
	)

	term := rf.Term
	localLeaderIdx := rf.VoteIdx
	serverStatus := rf.ServerStatus

	result := util.IntPtr(AppendEntriesResultSuccess)
	defer func() {
		switch *result {
		case AppendEntriesResultSuccess:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] allow [S%v] AppendEntries", rf.me, args.Term, rf.Term, args.LeaderIdx)
		case AppendEntriesResultDenyTermEqualDiffLeader:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] [L%v]->[L%v] deny [S%v] AppendEntries: leader not equal", rf.me,
				args.Term, rf.Term, args.LeaderIdx, localLeaderIdx, args.LeaderIdx)
		case AppendEntriesResultDenyTermEqualNowLeader:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] deney [S%v] AppendEntries:term equal but now is leader", rf.me, args.Term, rf.Term, args.LeaderIdx)
		case AppendEntriesResultDenyTermLess:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] deny [S%v] AppendEntries: term less than current term", rf.me, args.Term, rf.Term, args.LeaderIdx)
		case AppendEntriesResultDenyInvalidPrevLog:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] deny [S%v] AppendEntries: prev lognot equal [I%v]!=[I%v] [%v]->[%v]", rf.me, args.Term, rf.Term, args.LeaderIdx, args.PrevLogIdx, len(rf.Logs)-1, util.JSONMarshal(rf.Logs), util.JSONMarshal(args))
		default:
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] not found reason", rf.me, args.Term, rf.Term)
		}
	}()
	reply.Term, reply.Success = term, false
	reply.XTerm, reply.XLogLen, reply.XIdx = -1, -1, -1
	// term 小于当前term 直接返回
	if args.Term < term {
		result = util.IntPtr(AppendEntriesResultDenyTermLess)
		return
	}

	// Prev Log Term 不合法 当前server不存在PrevLogIdx的Log或者该log term不一致 这里要让leader主动重试
	if args.PrevLogIdx >= 0 && (args.PrevLogIdx > len(rf.Logs)-1 || rf.Logs[args.PrevLogIdx].Term != args.PrevLogTerm) {
		result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
		if args.PrevLogIdx > len(rf.Logs)-1 {
			reply.XLogLen = len(rf.Logs)
			return
		}
		if rf.Logs[args.PrevLogIdx].Term != args.PrevLogTerm {
			reply.XTerm = rf.Logs[args.PrevLogIdx].Term
			for termIdx := args.PrevLogIdx; termIdx >= 0; termIdx-- {
				if rf.Logs[termIdx].Term == rf.Logs[args.PrevLogIdx].Term {
					reply.XIdx = termIdx
				}
			}
			reply.XLogLen = len(rf.Logs)
			return
		}
		return
	}

	// Term相等
	if args.Term == rf.Term {
		// 当前Server的Leader非请求中的Server,拒绝
		if serverStatus == ServerStatusFollower && rf.VoteIdx != -1 && rf.VoteIdx != args.LeaderIdx {
			result = util.IntPtr(AppendEntriesResultDenyTermEqualDiffLeader)
			return
		}
		// 当前已经是leader，同步失败
		if serverStatus == ServerStatusLeader {
			result = util.IntPtr(AppendEntriesResultDenyTermEqualNowLeader)
			return
		}
	}

	/*
		1. term > 当前server term
		2. term == 当前server term
		2.1 当前状态为Follower && 与此次选举的leader一致
		2.2 当前状态为 Candidate
	*/

	rf.Term = args.Term
	rf.changeStatus(ServerStatusFollower, args.LeaderIdx)
	rf.VoteIdx = args.LeaderIdx
	reply.Success = true
	// 这里不仅要添加，还要清除
	for _, log := range args.Logs {
		for log.Idx > len(rf.Logs)-1 {
			rf.Logs = append(rf.Logs, nil)
		}
		rf.Logs[log.Idx] = log
	}
	if len(args.Logs) > 0 {
		rf.Logs = rf.Logs[:args.Logs[len(args.Logs)-1].Idx+1]
	}
	rf.CommitIdx(args.LeaderCommitIdx)
}

// SendHeartBeat
// TODO 发送心跳的时候也要携带 prevLogIdx prevLogTerm，因为收到消息的server会提交日志，所以需要通过prevLogIdx/Term保证日志的一致
func (rf *Raft) SendHeartBeat() {
	for rf.killed() == false {
		time.Sleep(time.Duration(rf.HeartbeatInterval * int(time.Millisecond)))
		rf.mu.Lock()
		// 更新CommitedIdx
		rf.UpdateCommitedIdx()
		status := rf.ServerStatus
		term := rf.Term
		committedIdx := rf.CommittedIdx
		logs := rf.Logs
		rf.mu.Unlock()

		if status != ServerStatusLeader {
			continue
		}

		replys := make([]*AppendEntriesReply, len(rf.peers))
		replyMutx := &sync.Mutex{}
		wg := sync.WaitGroup{}
		closeCh, successCnt := make(chan struct{}), int32(len(rf.peers)/2)
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			i, peer := i, peer

			wg.Add(1)
			go func() {
				defer wg.Done()
				for !rf.killed() {
					time.Sleep(time.Millisecond * 10)
					select {
					case <-closeCh:
						util.Logger.Printf("[HeartBeat] close [S%v] goroutine", i)
						return
					default:
						rf.mu.Lock()
						prevLogIdx, prevLogTerm := atomic.LoadInt32(&rf.NextIdxs[i])-1, -1
						if len(rf.Logs) > 0 && prevLogIdx >= 0 {
							prevLogTerm = rf.Logs[prevLogIdx].Term
						}
						args := &AppendEntriesArgs{
							LeaderIdx:       rf.me,
							Term:            term,
							PrevLogIdx:      int(prevLogIdx),
							PrevLogTerm:     prevLogTerm,
							LeaderCommitIdx: committedIdx,
						}
						// 传递数组须谨慎，这里如果直接使用rf.Logs[prevLogIdx+1:]，会有数据竞争
						if prevLogIdx >= -1 {
							args.Logs = append(args.Logs, rf.Logs[prevLogIdx+1:]...)
						}
						rf.mu.Unlock()

						util.Logger.Printf("[HeartBeat] [S%v]->[S%v] args=%v", rf.me, i, util.JSONMarshal(args))

						reply := &AppendEntriesReply{}
						ok := peer.Call("Raft.AppendEntries", args, reply)
						if !ok {
							util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, res=false", rf.me, i)
							continue
						}
						// term已经发生改变，应忽略此次心跳结果
						rf.mu.Lock()
						if term != rf.Term {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()

						if reply == nil {
							util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, reply is nil", rf.me, i)
							return
						}

						replyMutx.Lock()
						replys[i] = reply
						replyMutx.Unlock()
						if !reply.Success && prevLogIdx >= 0 {
							if reply.XTerm != -1 {
								rf.mu.Lock()
								firstCommand := rf.findTermFirstCommand(reply.Term)
								rf.mu.Unlock()
								if firstCommand != nil {
									atomic.StoreInt32(&rf.NextIdxs[i], int32(firstCommand.Idx))
									util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, reduce prevLogIdx->%v", rf.me, i, firstCommand.Idx)
								} else {
									atomic.StoreInt32(&rf.NextIdxs[i], int32(reply.XIdx))
									util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, reduce prevLogIdx->%v", rf.me, i, reply.XIdx)
								}
							} else if reply.XLogLen != -1 {
								atomic.StoreInt32(&rf.NextIdxs[i], int32(reply.XLogLen))
								util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, reduce prevLogIdx->%v", rf.me, i, reply.XLogLen)
							} else {
								atomic.StoreInt32(&rf.NextIdxs[i], prevLogIdx)
								util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, reduce prevLogIdx->%v", rf.me, i, prevLogIdx)
							}
							continue
						}
						if !reply.Success && prevLogIdx < 0 {
							util.Logger.Printf("[HeartBeat] [S%v]->[S%v] fail, can't reduce prevLogIdx", rf.me, i)
							return
						}
						// success
						// 更新NextIdx以及MatchIdxs
						rf.mu.Lock()
						atomic.StoreInt32(&rf.NextIdxs[i], int32(len(logs)))
						rf.MatchIdxs[i] = int32(len(logs) - 1)
						util.Logger.Printf("[HeartBeat] success, update NextIdxs[%v]->%v, MatchIdxs[%v]->%v", i, len(logs), i, len(logs)-1)
						rf.mu.Unlock()
						atomic.AddInt32(&successCnt, -1)
						return
					}
				}
			}()
		}
		// TODO 最新的问题，由于有长时间的RPC导致超时，这里一直无法收到足够的心跳，导致Log同步失败
		// 这里的超时时间设置的太短了，应该加上多数server统一就返回的逻辑，然后再增大超时时间
		// TODO 这里收到大多数的回应就可以继续保持leader了，不需要等待超时
		// TODO 新问题，Start应该什么时候返回OK
		// 这里可以不用等待watigroup结束
		util.WaitWithTimeout(&wg, time.Duration(int(rf.ElectionTimeout)*int(time.Millisecond)), &successCnt)
		close(closeCh)
		// term已经发生改变，应忽略此次心跳结果
		rf.mu.Lock()
		if term != rf.Term {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		count := 0
		maxTerm := term
		replyMutx.Lock()
		for _, reply := range replys {
			if reply == nil {
				continue
			}
			if reply.Success {
				count++
			}
			maxTerm = util.Max(maxTerm, reply.Term)
		}
		replyMutx.Unlock()
		// 更新Term
		if maxTerm > term {
			rf.mu.Lock()
			if maxTerm > rf.Term {
				rf.Term = maxTerm
				rf.changeStatus(ServerStatusFollower, -1)
			}
			rf.mu.Unlock()
			continue
		}
		if count >= len(rf.peers)/2 {
			util.Logger.Printf("[HeartBeat] [S%v] receive major heartbeat", rf.me)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusLeader, rf.me)
			rf.mu.Unlock()
		} else {
			util.Logger.Printf("[HeartBeat] [S%v] not receive major heartbeat", rf.me)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusCandidate, rf.me)
			rf.mu.Unlock()
		}
	}
	util.Logger.Printf("[SendHeartBeat] [S%v] stop send heart beat", rf.me)
}

// 根据MatchIdxs更新commitedIdx, 须持有锁
// 这里server的数量不会太大直接双重for循环计算
func (rf *Raft) UpdateCommitedIdx() {
	if rf.ServerStatus != ServerStatusLeader {
		return
	}
	util.Logger.Printf("[UpdateCommitedIdx] [S%v] MatchIdxs=%v", rf.me, util.JSONMarshal(rf.MatchIdxs))
	maxIdx := -1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		cnt := 0
		for j := range rf.peers {
			if j == i || j == rf.me {
				continue
			}
			if rf.MatchIdxs[j] >= rf.MatchIdxs[i] {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2-1 && int(rf.MatchIdxs[i]) > maxIdx && int(rf.MatchIdxs[i]) < len(rf.Logs) {
			maxIdx = int(rf.MatchIdxs[i])
		}
	}
	canCommitIdx := -1
	for i := maxIdx; i > rf.CommittedIdx; i-- {
		if rf.Logs[i].Term == rf.Term {
			canCommitIdx = i
			break
		}
	}
	if canCommitIdx > rf.CommittedIdx {
		rf.CommitIdx(canCommitIdx)
	}
}

type AppendEntriesArgs struct {
	Term      int
	LeaderIdx int // 这里叫ServerIdx比较好

	PrevLogIdx      int
	PrevLogTerm     int
	Logs            []*Log
	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Optimize
	XTerm   int
	XIdx    int
	XLogLen int
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rand.Seed(time.Now().UnixNano())

	// Your initialization code here (2A, 2B, 2C).
	// Lab强制每秒限制10次心跳，心跳频率跟选举超时时间无关。
	// 需要保证在一次选举超时时间内可以完整的进行多次心跳即可。
	// 选举超时时间为 400~600ms
	// 这里先调整大一点，2C中有个实验RPC的延迟达到了600多ms
	rf.ElectionTimeout = int32(rand.Intn(200) + 600)
	// 初始化心跳时间
	rf.LastHeartbeatTime = time.Now().UnixMilli()
	// 初始状态为 follower
	rf.ServerStatus = ServerStatusFollower
	// 每秒 10 次心跳
	rf.HeartbeatInterval = 100 // ms
	// 初始化
	rf.VoteIdx = -1
	rf.CommittedIdx = -1
	rf.Term = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ApplyMsgCh = applyCh
	util.Logger.Printf("[Init] [S%v] lastHeartbeatTime:%v", rf.me, rf.LastHeartbeatTime)
	util.Logger.Printf("[Init] [S%v] electionTimeout:%v", rf.me, rf.ElectionTimeout)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.SendHeartBeat()

	return rf
}
