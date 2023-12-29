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
	// 2D
	SnapshotInfo  *SnapshotInfo // 快照信息
	LogIdxMapping map[int]int   // Key=Command.Idx Value=Command在rf.Logs中的Idx
}

type SnapshotInfo struct {
	Data             []byte
	LastIncludedIdx  int
	LastIncludedTerm int
}

func (rf *Raft) findTermLastCommand(term int) *Log {
	var res *Log
	for i := 0; i < len(rf.Logs); i++ {
		if rf.Logs[i] == nil {
			util.Logger.Printf("findTermLastCommand [S%v] rf.Logs[%v]=nil", rf.me, i)
			continue
		}
		if rf.Logs[i].Term == term {
			res = rf.Logs[i]
		}
		if rf.Logs[i].Term > term {
			break
		}
	}

	return res
}

// getLastIncludeTermAndIdx 获取当前server中最后一个Log的Index以及Term
func (rf *Raft) getLastIdxAndTerm() (int, int) {
	lastLogIdx, lastLogTerm := -1, -1
	if len(rf.Logs) > 0 {
		lastLogIdx, lastLogTerm = util.Last(rf.Logs).Idx, util.Last(rf.Logs).Term
	} else if rf.SnapshotInfo != nil {
		lastLogIdx, lastLogTerm = rf.SnapshotInfo.LastIncludedIdx, rf.SnapshotInfo.LastIncludedTerm
	}

	return lastLogIdx, lastLogTerm
}

// getLogByIdx 获取指定idx的Log
func (rf *Raft) getLogByIdx(idx int) (int, *Log) {
	logIdx, ok := rf.LogIdxMapping[idx]
	if !ok {
		return -1, nil
	}
	return logIdx, rf.Logs[logIdx]
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
	lastLogIdx, _ := rf.getLastIdxAndTerm()
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIdxs[i] = int32(lastLogIdx) + 1
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
	Term          int
	VoteIdx       int
	Logs          []*Log
	CommittedIdx  int
	LogIdxMapping map[int]int
	SnapshotInfo  *SnapshotInfo
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
		Term:          rf.Term,
		VoteIdx:       rf.VoteIdx,
		Logs:          rf.Logs,
		CommittedIdx:  rf.CommittedIdx,
		LogIdxMapping: rf.LogIdxMapping,
		SnapshotInfo:  rf.SnapshotInfo,
	}
	encoder.Encode(persistState)
	raftState := buf.Bytes()
	rf.persister.Save(raftState, nil)
	//util.Logger.Printf("[persist] [S%v] success:%v", rf.me, util.JSONMarshal(persistState))
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
	rf.LogIdxMapping = persistState.LogIdxMapping
	rf.SnapshotInfo = persistState.SnapshotInfo
	util.Logger.Printf("[readPersist] [S%v] success: %v", rf.me, util.JSONMarshal(persistState))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	util.Logger.Printf("[S%v] [T%v] Snapshot %v", rf.me, rf.Term, index)
	log := rf.Logs[index]
	snapshotInfo := &SnapshotInfo{
		Data:             snapshot,
		LastIncludedIdx:  index,
		LastIncludedTerm: log.Term,
	}
	rf.SnapshotInfo = snapshotInfo
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 选举人参加选举的任期
	ServerIdx   int // 选举人的下标
	LastLogIdx  int // 最后一条log的下标
	LastLogTerm int // 最后一条log的term

	// ForDebug
	FromServer int
	ToServer   int
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

TODO  TestFailNoAgree2B中这段代码实现好像有点问题，比如执行完336行之后,leader2因为某些原因不再是leader，这里338的判断就不对了
	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}


*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//util.Logger.Printf("[RequestVote] [S%v] [T%v] recv [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, args.Term)
	defer func() {
		if reply.IsVote {
			util.Logger.Printf("[RequestVote] [S%v] [T%v] vote for [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, args.Term)
		} else {
			util.Logger.Printf("[RequestVote] [S%v] [T%v] deny [S%v] [T%v] RequestVote:%v", rf.me, rf.Term, args.ServerIdx, args.Term, reply.DenyReason)
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
	lastLogIdx, lastLogTerm := rf.getLastIdxAndTerm()
	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.changeStatus(ServerStatusFollower, -1, false)
		// Last Log比较
		if args.LastLogTerm < lastLogTerm {
			reply.DenyReason = fmt.Sprintf("last log term less: [T%v] < [T%v]", args.LastLogTerm, lastLogTerm)
			return
		}
		if args.LastLogTerm == lastLogTerm && args.LastLogIdx < lastLogIdx {
			reply.DenyReason = fmt.Sprintf("last log idx less: [I%v] < [I%v]", args.LastLogIdx, lastLogIdx)
			return
		}
		reply.IsVote = true
		rf.changeStatus(ServerStatusFollower, args.ServerIdx, true)
		return
	}

	if args.Term == rf.Term {
		// Last Log比较
		if args.LastLogTerm < lastLogTerm {
			reply.DenyReason = fmt.Sprintf("last log term less: [T%v] < [T%v]", args.LastLogTerm, lastLogTerm)
			return
		}
		if args.LastLogTerm == lastLogTerm && args.LastLogIdx < lastLogIdx {
			reply.DenyReason = fmt.Sprintf("last log idx less: [I%v] < [I%v]", args.LastLogIdx, lastLogIdx)
			return
		}
		if rf.VoteIdx == -1 || rf.VoteIdx == args.ServerIdx {
			reply.IsVote = true
			rf.changeStatus(ServerStatusFollower, args.ServerIdx, true)
		} else {
			reply.DenyReason = fmt.Sprintf("already vote for %v", rf.VoteIdx)
		}
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderIdx        int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

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

	// Your code here (2B).
	// TODO 这里直接锁整个函数
	rf.mu.Lock()
	isLeader := rf.ServerStatus == ServerStatusLeader
	rf.mu.Unlock()
	// 非Leader直接返回
	if !isLeader {
		return -1, -1, false
	}
	rf.mu.Lock()
	util.Logger.Printf("[Start] [S%v] [T%v] command = %v", rf.me, rf.Term, util.JSONMarshal(command))
	term := rf.Term
	lastLogIdx, _ := rf.getLastIdxAndTerm()
	log := &Log{
		Term:    term,
		Idx:     lastLogIdx + 1,
		Command: command,
	}
	rf.LogIdxMapping[log.Idx] = len(rf.Logs)
	rf.Logs = append(rf.Logs, log)
	rf.mu.Unlock()

	return log.Idx + 1, log.Term, true
}

// CommitIdx 执行CommitIdx需在外层获取rf.mu
func (rf *Raft) CommitIdx(idx int) {
	//util.Logger.Printf("[CommitIdx] [S%v] commit %v", rf.me, idx)
	if idx <= rf.CommittedIdx {
		//util.Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v <= rf.CommitedIdx %v, not need commit", rf.me, rf.Term, idx, rf.CommittedIdx)
		return
	}
	lastLogIdx, _ := rf.getLastIdxAndTerm()
	idx = util.Min(idx, lastLogIdx)
	//util.Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v", rf.me, rf.Term, idx)
	startIdx, _ := rf.getLogByIdx(rf.CommittedIdx + 1)
	endIdx, _ := rf.getLogByIdx(idx)
	for i := startIdx; i <= endIdx; i++ {
		rf.ApplyMsgCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[i].Command,
			CommandIndex: rf.Logs[i].Idx + 1,
		}
	}
	rf.CommittedIdx = idx
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
	/*
		TODO 2D
		lastLog只是用来取LastIncludedIdx以及LastIncludedTerm
	*/
	lastLogIdx, lastLogTerm := rf.getLastIdxAndTerm()
	rf.mu.Unlock()

	// 2. 向其他服务器发起投票
	wg, replys, needVoteCnt := sync.WaitGroup{}, make([]*RequestVoteReply, len(rf.peers)), int32(len(rf.peers)/2)
	replyMu := sync.Mutex{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &RequestVoteArgs{
				Term:        localTerm,
				ServerIdx:   rf.me,
				LastLogIdx:  lastLogIdx,
				LastLogTerm: lastLogTerm,

				//FromServer: rf.me,
				//ToServer:   i,
			}
			reply := &RequestVoteReply{}
			util.Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote...", rf.me, localTerm, i)
			res := rf.sendRequestVote(i, args, reply)
			if !res {
				util.Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote fail", rf.me, localTerm, i)
				return
			}
			replyMu.Lock()
			replys[i] = reply
			if reply != nil && reply.IsVote {
				atomic.AddInt32(&needVoteCnt, -1)
			}
			replyMu.Unlock()
		}()
	}
	// 这里不能傻傻等待所有服务器响应，只要超过一半的server投票，就应该停止
	util.WaitWithTimeout(&wg, time.Millisecond*time.Duration(rf.ElectionTimeout)*3/5, &needVoteCnt)
	// 判断是否存在ServerTerm大于当前
	var maxTerm int
	replyMu.Lock()
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
	replyMu.Unlock()
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

	if atomic.LoadInt32(&needVoteCnt) <= 0 {
		util.Logger.Printf("[Election] [S%v] [T%v] receive major vote[%v]", rf.me, rf.Term, int32(len(rf.peers)/2)-atomic.LoadInt32(&needVoteCnt))
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
	if rf.ServerStatus != toStatus {
		util.Logger.Printf("[changeStatus] [S%v] change server status [%v]->[%v]", rf.me, rf.ServerStatus, toStatus)
	}
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

const (
	AppendEntriesResultSuccess                 = 1
	AppendEntriesResultDenyTermLess            = 2
	AppendEntriesResultDenyTermEqualDiffLeader = 3
	AppendEntriesResultDenyTermEqualNowLeader  = 4
	AppendEntriesResultDenyInvalidPrevLog      = 5
)

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

这里如果是因为日志不匹配导致的，这里应该也要重置计时器，给当前leader再次发送的时机
2D:

	1. prevLogIdx < Snapshot.LastIncludedIdx 可能性分析
		1.1 Leader初始化时prevLogIdx是Leader的Log长度，初始的prevLogIdx一定大于其余Server的LastIncludedIdx
		1.2 prevLogIdx变小的情况
			1.2.1 prevLogIdx>Follower的Log长度
				prevLogIdx会变为len(Follower.Logs),而len(Follower.Logs)>=Follower.SnapshotInfo.LastIncludedIdx
			1.2.2 prevLogTerm!=Follower.Logs[prevLogIdx].Term
				prevLogIdx会变为Follower.Logs[prevLogIdx].Term最早的一条Log,由于引入了SnapshotInfo,所以最小也只能等于SnapshotInfo.LastIncludedIdx
		结论:正常情况下不可能,当存在网络延迟时,过时的请求可能会出现此问题,此时处于安全考虑,应该拒绝
	2. prevLogIdx > SnapshotInfo.LastIncludedIdx
		此时就与2B中的情况一样,正常处理
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// util.Logger.Printf("[AppendEntries] [S%v] [T%v] recv [S%v] [T%v]", rf.me, rf.Term, args.LeaderIdx, args.Term)

	term := rf.Term
	localLeaderIdx := rf.VoteIdx
	serverStatus := rf.ServerStatus

	result := util.IntPtr(AppendEntriesResultSuccess)
	defer func() {
		reply.Result = *result
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
			util.Logger.Printf("[AppendEntries] [S%v] [T%v]->[T%v] deny [S%v] AppendEntries: prev log not equal\nleader log=%v\nfollower log=%v\nrpc logs=%v",
				rf.me, args.Term, rf.Term, args.LeaderIdx, util.JSONMarshal(args.AllLogs), util.JSONMarshal(rf.Logs), util.JSONMarshal(args.Logs))
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

	// Term大于
	// PrevLogIdx<快照
	if rf.SnapshotInfo != nil && args.PrevLogIdx < rf.SnapshotInfo.LastIncludedIdx {
		rf.changeStatus(ServerStatusFollower, args.LeaderIdx, true)
		result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
		reply.XLogLen = rf.SnapshotInfo.LastIncludedIdx + 1
		return
	}
	if rf.SnapshotInfo != nil && args.PrevLogIdx == rf.SnapshotInfo.LastIncludedIdx {
		if args.PrevLogTerm != rf.SnapshotInfo.LastIncludedTerm {
			rf.changeStatus(ServerStatusFollower, args.LeaderIdx, true)
			result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
			reply.XLogLen = rf.SnapshotInfo.LastIncludedIdx + 1
			return
		}
	}
	// rf.SnapshotInfo == nil || (rf.SnapshotInfo != nil && args.PrevLogIdx > rf.SnapshotInfo.LastIncludedIdx)
	lastLogIdx, _ := rf.getLastIdxAndTerm()
	prevLogIdx, prevLog := rf.getLogByIdx(args.PrevLogIdx)
	if args.PrevLogIdx >= 0 && prevLog == nil {
		util.Logger.Printf("args.PrevLogIdx=%v, prevLog==nil", args.PrevLogIdx)
		rf.changeStatus(ServerStatusFollower, args.LeaderIdx, true)
		result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
		reply.XLogLen = lastLogIdx + 1
		return
	}
	if prevLog != nil && prevLog.Term != args.PrevLogTerm {
		rf.changeStatus(ServerStatusFollower, args.LeaderIdx, true)
		result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
		reply.XTerm = rf.Logs[args.PrevLogIdx].Term
		for termIdx := prevLogIdx; termIdx >= 0; termIdx-- {
			if rf.Logs[termIdx].Term == rf.Logs[prevLogIdx].Term {
				reply.XIdx = rf.Logs[termIdx].Idx
			}
		}
		if reply.XIdx == -1 {
			reply.XIdx = rf.SnapshotInfo.LastIncludedIdx + 1
		}
		// TODO 这里后续使用Snapshot之后要修改
		reply.XLogLen = len(rf.Logs)
		return
	}

	/*
		1. term > 当前server term
		2. term == 当前server term
		2.1 当前状态为Follower && 与此次选举的leader一致
		2.2 当前状态为 Candidate
	*/

	rf.Term = args.Term
	rf.changeStatus(ServerStatusFollower, args.LeaderIdx, true)
	rf.VoteIdx = args.LeaderIdx
	reply.Success = true
	flag := false
	// 这里不仅要添加，还要清除
	lastUpdateLogIdx := -1
	for _, argLog := range args.Logs {
		logIdx, log := rf.getLogByIdx(argLog.Idx)
		if log == nil {
			rf.Logs = append(rf.Logs, argLog)
			rf.LogIdxMapping[argLog.Idx] = len(rf.Logs) - 1
			continue
		}
		// Log存在冲突时才需要删除后面的元素
		if log.Term != argLog.Term {
			flag = true
		}
		rf.Logs[logIdx] = argLog
		rf.LogIdxMapping[argLog.Idx] = logIdx
		lastUpdateLogIdx = logIdx
	}

	// 删除冲突的Log
	if flag && lastUpdateLogIdx != -1 {
		for _, log := range rf.Logs[lastUpdateLogIdx+1:] {
			if log != nil {
				delete(rf.LogIdxMapping, log.Idx)
			}
		}
		rf.Logs = rf.Logs[:lastUpdateLogIdx+1]
	}
	rf.CommitIdx(args.LeaderCommitIdx)
	rf.persist()
}

// SendHeartBeat 发送心跳
/*
1. 非Leader返回
*/
func (rf *Raft) SendHeartBeat() {
	for !rf.killed() {
		time.Sleep(time.Duration(rf.HeartbeatInterval * int(time.Millisecond)))
		rf.mu.Lock()
		if time.Now().UnixMilli()-rf.LastHeartbeatTime >= int64(rf.ElectionTimeout) {
			rf.changeStatus(ServerStatusFollower, -1, false)
		}
		status := rf.ServerStatus
		rf.mu.Unlock()
		if status != ServerStatusLeader {
			continue
		}
		go rf.SendAppendEntries()
	}
	util.Logger.Printf("[SendHeartBeat] [S%v] stop send heart beat", rf.me)
}

func (rf *Raft) SendAppendEntries() bool {
	rf.mu.Lock()
	term := rf.Term
	rf.mu.Unlock()

	rf.mu.Lock()
	util.Logger.Printf("[SendAppendEntries]NextIdxs=%v", util.JSONMarshal(rf.NextIdxs))
	util.Logger.Printf("[SendAppendEntries]MatchIdxs=%v", util.JSONMarshal(rf.MatchIdxs))
	rf.mu.Unlock()

	successCnt, replys, closeCh := int32(len(rf.peers)/2), make([]*AppendEntriesReply, len(rf.peers)), make(chan struct{})
	wg, replyMu := sync.WaitGroup{}, sync.Mutex{}
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		i, peer := i, peer
		wg.Add(1)
		/*
			TODO 2D
			1. prevLogIdx<当前快照的LastLogIdx应该先阻塞并发送快照
			2. 如何快速获取prevLogIdx之后的Log,建立一个mapping放入到args

		*/
		go func() {
			defer wg.Done()
			for !rf.killed() {
				select {
				case <-closeCh:
					return
				default:
					rf.mu.Lock()
					rf.UpdateCommitedIdx()
					committedIdx := rf.CommittedIdx
					allLogs := rf.Logs

					if rf.ServerStatus != ServerStatusLeader {
						rf.mu.Unlock()
						return
					}

					var (
						logs        []*Log
						prevLogIdx      = int(rf.getNextIdx(i) - 1)
						prevLogTerm int = -1
					)
					realPrevLogIdx, prevLog := rf.getLogByIdx(prevLogIdx)
					if prevLogIdx >= -1 && realPrevLogIdx < len(allLogs) {
						logs = append([]*Log{}, allLogs[realPrevLogIdx+1:]...)
					}
					if prevLog != nil {
						prevLogTerm = prevLog.Term
					}
					req := &AppendEntriesArgs{
						Term:            term,
						LeaderIdx:       rf.me,
						PrevLogIdx:      prevLogIdx,
						PrevLogTerm:     prevLogTerm,
						Logs:            logs,
						LeaderCommitIdx: committedIdx,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := peer.Call("Raft.AppendEntries", req, reply)
					if !ok {
						continue
					}

					replyMu.Lock()
					replys[i] = reply
					replyMu.Unlock()

					rf.mu.Lock()
					nowTerm := rf.Term
					rf.mu.Unlock()
					if nowTerm != term {
						return
					}

					if util.ContainsInt([]int{
						AppendEntriesResultDenyTermLess,
						AppendEntriesResultDenyTermEqualDiffLeader,
						AppendEntriesResultDenyTermEqualNowLeader,
					}, reply.Result) {
						return
					}

					// TODO 这里似乎要配合修改一下
					if !reply.Success {
						rf.mu.Lock()
						if reply.XTerm != -1 {
							firstCommand := rf.findTermLastCommand(reply.XTerm)
							if firstCommand != nil {
								rf.storeNextIdx(i, int32(firstCommand.Idx))
							} else {
								rf.storeNextIdx(i, int32(reply.XIdx))
							}
						} else if reply.XLogLen != -1 {
							rf.storeNextIdx(i, int32(reply.XLogLen))
						} else if prevLogIdx >= 0 {
							rf.storeNextIdx(i, int32(prevLogIdx))
						}
						rf.mu.Unlock()
						continue
					}

					rf.mu.Lock()
					if len(logs) > 0 {
						rf.storeNextIdx(i, int32(util.Last(logs).Idx)+1)
						rf.storeMatchIdx(i, int32(util.Last(logs).Idx))
					}
					atomic.AddInt32(&successCnt, -1)
					rf.mu.Unlock()
					return
				}
			}
		}()
	}
	timeout := util.WaitWithTimeout(&wg, time.Duration(rf.ElectionTimeout)*time.Millisecond, &successCnt)
	if timeout {
		util.Logger.Printf("[SendAppendEntries] [S%v] [T%v] timeout", rf.me, term)
	} else {
		util.Logger.Printf("[SendAppendEntries] [S%v] [T%v] success", rf.me, term)
	}
	close(closeCh)

	rf.mu.Lock()
	nowTerm := rf.Term
	rf.mu.Unlock()
	if nowTerm != term {
		return false
	}

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
		rf.mu.Unlock()
		return false
	}

	if atomic.LoadInt32(&successCnt) <= 0 {
		rf.mu.Lock()
		rf.changeStatus(ServerStatusLeader, rf.me)
		rf.mu.Unlock()
		return true
	}

	return false
}

func (rf *Raft) getNextIdx(i int) int32 {
	return atomic.LoadInt32(&rf.NextIdxs[i])
}

// storeNextIdx 调用此函数需要加锁，因为在UpdateCommitIndex中会encode NextIdx数组
func (rf *Raft) storeNextIdx(i int, value int32) {
	atomic.StoreInt32(&rf.NextIdxs[i], value)
	util.Logger.Printf("[storeNextIdx] [S%v] update-> [%v]", i, value)
}

// storeMatchIdx 调用此函数需要加锁，因为在UpdateCommitIndex中会encode MatchIdxs数组
func (rf *Raft) storeMatchIdx(i int, value int32) {
	atomic.StoreInt32(&rf.MatchIdxs[i], value)
	util.Logger.Printf("[storeMatchIdx] [S%v] update-> [%v]", i, value)
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
			if j == rf.me {
				continue
			}
			if rf.MatchIdxs[j] >= rf.MatchIdxs[i] {
				cnt++
			}
		}
		// TODO 2D 这里的判断需要改下 直接去掉后面的长度判断？
		if cnt >= len(rf.peers)/2 && int(rf.MatchIdxs[i]) > maxIdx && int(rf.MatchIdxs[i]) < len(rf.Logs) {
			maxIdx = int(rf.MatchIdxs[i])
		}
	}
	canCommitIdx := -1
	// TODO 这里也要改下
	// 只有当前任期的Log会被提交，是为了解决Raft论文中Figure8导致的问题，同时又引入了一个新问题
	// 如果log被复制到大多数server，leader在提交这条log之前crash掉了，新上任的leader如果没有添加新Log的机会
	// 前任leader的最后一条log永远不会被commit
	// Raft针对这种场景的解决方式是Leader上任之后添加一条no-op的特殊Log，用于安全提交上一任的Log
	for i := maxIdx; i > rf.CommittedIdx; i-- {
		if rf.Logs[i].Term == rf.Term {
			canCommitIdx = i
			break
		}
	}
	if canCommitIdx > rf.CommittedIdx {
		rf.CommitIdx(canCommitIdx)
		rf.persist()
	}
}

type AppendEntriesArgs struct {
	Term      int
	LeaderIdx int // 这里叫ServerIdx比较好

	PrevLogIdx      int
	PrevLogTerm     int
	Logs            []*Log
	LeaderCommitIdx int
	// For Debug
	AllLogs    []*Log
	FromServer int
	ToServer   int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Result  int

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
	rf.ElectionTimeout = int32(rand.Intn(200) + 400)
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
	rf.LogIdxMapping = make(map[int]int)
	// initialize from state persisted before a crash
	// 这一行一定要位于最下方
	rf.readPersist(persister.ReadRaftState())
	rf.ApplyMsgCh = applyCh
	util.Logger.Printf("[Init] [S%v] lastHeartbeatTime:%v", rf.me, rf.LastHeartbeatTime)
	util.Logger.Printf("[Init] [S%v] electionTimeout:%v", rf.me, rf.ElectionTimeout)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.SendHeartBeat()

	return rf
}
