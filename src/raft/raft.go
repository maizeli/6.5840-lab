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
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/util"
)

var Logger *log.Logger

func init() {
	file, err := os.Create(fmt.Sprintf("log-%v.txt", time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	Logger = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
}

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
	LeaderIdx         int          // leader下标
	ServerStatus      ServerStatus // 当前状态
	HeartbeatInterval int          // 每秒发送的心跳包
	LastHeartbeatTime int64        // 上次心跳时间 单位:ms
	// 2B
	Logs       []*Log
	NextIdxMap sync.Map // 这里每次当选leader时应该被置为当前leader的值？
	ApplyMsgCh chan ApplyMsg
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
// 1. 重置NextIdxMap
func (rf *Raft) LeaderInit() {
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIdxMap.Store(i, len(rf.Logs))
	}
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	CurrentTerm int // 选举人参加选举的任期
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
	reply.ServerIdx = rf.me
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Logger.Printf("[Election] [%v] [T%v]->[T%v] [S%v]->[S%v] recv request vote",
		time.Now().UnixMilli(), args.CurrentTerm, rf.Term, args.ServerIdx, rf.me)
	defer func() {
		if reply.IsVote {
			Logger.Printf("[Election] [%v] [T%v]->[T%v] [S%v]->[S%v] vote for this request",
				time.Now().UnixMilli(), args.CurrentTerm, rf.Term, args.ServerIdx, rf.me)
		} else {
			Logger.Printf("[Election] [%v] [T%v]->[T%v] [S%v]->[S%v] deny this request",
				time.Now().UnixMilli(), args.CurrentTerm, rf.Term, args.ServerIdx, rf.me)
		}
	}()
	reply.Term = rf.Term
	// 1. 过期Term
	if args.CurrentTerm < rf.Term {
		reply.IsVote = false
		return
	}
	// Last Log比较
	lastLogTerm, lastLogIdx := 0, 0
	if len(rf.Logs) > 0 {
		lastLog := rf.Logs[len(rf.Logs)-1]
		lastLogTerm = lastLog.Term
		lastLogIdx = lastLog.Idx
	}
	if args.LastLogTerm < lastLogTerm {
		return
	}
	if args.LastLogTerm == lastLogTerm && args.LastLogIdx < lastLogIdx {
		return
	}

	// 1. 更新当前的任期以及是否投过票等信息
	if args.CurrentTerm > rf.Term {
		rf.changeStatus(ServerStatusFollower, args.ServerIdx)
		reply.IsVote = true
		rf.Term = args.CurrentTerm
		return
	}

	if rf.VoteIdx == -1 || rf.VoteIdx == args.ServerIdx {
		rf.changeStatus(ServerStatusFollower, args.ServerIdx)
		reply.IsVote = true
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	Logger.Printf("[Start] command =%v", util.JSONMarshal(command))
	idx := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.ServerStatus == ServerStatusLeader
	// 非Leader直接返回
	if !isLeader {
		Logger.Printf("[Start] %v [T%v] [S%v] not leader", time.Now().UnixMilli(), rf.Term, rf.me)
		rf.mu.Unlock()
		return idx, term, isLeader
	}
	idx = len(rf.Logs)
	term = rf.Term
	// 将command添加到leader本地状态机
	rf.Logs = append(rf.Logs, &Log{
		Term:    term,
		Idx:     len(rf.Logs),
		Command: command,
	})
	rf.ApplyMsgCh <- ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: idx, // TODO test中index从1开始，发送消息以及返回给上游都+1，实际存储还是0开始
	}
	logs := rf.Logs
	rf.mu.Unlock()

	wg := &sync.WaitGroup{}
	// Leader，开始AppendEntries
	cnt := int32(0)
	for i, peer := range rf.peers {
		i := i
		if i == rf.me {
			continue
		}
		peer := peer
		wg.Add(1)

		go func() {
			defer wg.Done()
			// 死循环复制
			for {
				// 这里 term 需要初始化为0，防止第一次同步失败
				prevLogIdx, prevLogTerm := -1, -1
				if nextIdxAny, ok := rf.NextIdxMap.Load(i); ok {
					prevLogIdx = nextIdxAny.(int) - 1
				}
				if len(logs) > 0 && prevLogIdx >= 0 {
					prevLogTerm = logs[prevLogIdx].Term
				}
				args := &AppendEntriesArgs{
					Term:        term,
					LeaderIdx:   rf.me,
					PrevLogIdx:  prevLogIdx,
					PrevLogTerm: prevLogTerm,
				}
				if prevLogIdx+1 >= 0 {
					args.Logs = logs[prevLogIdx+1:]
				}
				Logger.Printf("[Start] send AppendEntries to [S%v] [T%v] args=%v", i, term, util.JSONMarshal(args))
				reply := &AppendEntriesReply{}
				res := peer.Call("Raft.AppendEntries", args, reply)
				if !res {
					return
				}
				// 如果出现并发更新可能会导致leader丢失一部分数据
				if reply.Success {
					atomic.AddInt32(&cnt, 1)
					cnt++
					rf.NextIdxMap.Store(i, len(logs))
					Logger.Printf("[Start] %v [T%v] [S%v] send AppendEntries to [S%v] success", time.Now().UnixMilli(), term, rf.me, i)
					break
				} else {
					rf.NextIdxMap.Store(i, prevLogIdx-1)
					prevLogIdx--
					Logger.Printf("[Start] %v [T%v] [S%v] send AppendEntries to [S%v] fail, retry", time.Now().UnixMilli(), term, rf.me, i)
					if prevLogIdx == -1 {
						Logger.Printf("[Start] %v [T%v] [S%v] prev log idx is -1", time.Now().UnixMilli(), term, rf.me)
						break
					}
				}
			}
		}()
	}
	// TODO 超时时长待确定
	timeOut := util.WaitWithTimeout(wg, time.Second)
	if timeOut {
		Logger.Printf("[Start] %v [T%v] time out", time.Now().UnixMilli(), term)
	}
	if int(cnt) <= len(rf.peers)/2 {
		Logger.Printf("[Start] %v [T%v] less than half server aggree command %v", time.Now().UnixMilli(), term, command)
	} else {
		Logger.Printf("[Start] %v [T%v] more than half server aggree command %v", time.Now().UnixMilli(), term, command)
	}

	return idx, term, isLeader
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
	Logger.Printf("[Election] [%v] [T%v] [S%v] start election",
		time.Now().UnixMilli(), rf.Term, rf.me)
	// 1. 给自己投一票
	res := rf.changeStatus(ServerStatusCandidate, rf.me)
	if !res {
		rf.mu.Unlock()
		return
	}
	rf.Term += 1
	localTerm := rf.Term
	var lastCommand *Log
	if len(rf.Logs) > 0 {
		lastCommand = rf.Logs[len(rf.Logs)-1]
	}
	rf.mu.Unlock()

	// 2. 向其他服务器发起投票
	wg := sync.WaitGroup{}
	replys := make([]*RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := &RequestVoteArgs{
				CurrentTerm: localTerm,
				ServerIdx:   rf.me,
			}
			if lastCommand != nil {
				args.LastLogIdx = lastCommand.Idx
				args.LastLogTerm = lastCommand.Term
			}
			reply := &RequestVoteReply{}
			res := rf.sendRequestVote(i, args, reply)
			if !res {
				Logger.Printf("[Election] [%v] [T%v] [S%v]->[S%v] send request vote fail", time.Now().UnixMilli(), rf.Term, rf.me, i)
				return
			}
			replys[i] = reply
		}()
	}
	Logger.Printf("[Election] [%v] [T%v] [S%v] send request vote success, begin waiting....", time.Now().UnixMilli(), rf.Term, rf.me)
	// 这里不能傻傻等待
	// 3. 等待响应，如果超过选举超时时间，结束等待
	// 这里超时不能直接返回,超时时有可能已经收到大部分的投票
	// 超时时间选择选举超时时间是为了不阻塞下次选举
	util.WaitWithTimeout(&wg, time.Millisecond*time.Duration(rf.ElectionTimeout))
	mp := map[int32]bool{}
	var maxTerm int
	for _, reply := range replys {
		if reply == nil {
			continue
		}
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		if !reply.IsVote {
			continue
		}
		Logger.Printf("[Election] [%v] [S%v] receive vote from [S%v]", time.Now().UnixMilli(), rf.me, reply.ServerIdx)
		mp[int32(reply.ServerIdx)] = true
	}
	// 4. 统计结果
	/*
		中间可能有意外发生:
		1. 选举过程中收到其他 leader 的心跳，此时变为Follower
	*/
	rf.mu.Lock()
	// 此时term已经过期 已经变成其他人的小弟or已经开始下一轮选举
	if rf.Term != localTerm {
		Logger.Printf("[Election] [%v] [T%v] [S%v] term is not equal, current term is [%v]", time.Now().UnixMilli(), rf.Term, rf.me, localTerm)
		// 快速纠正当前term
		if maxTerm > rf.Term {
			rf.Term = maxTerm
		}
		rf.mu.Unlock()
		return
	}
	defer rf.mu.Unlock()
	// 包含等于:因为自己会给自己投一票
	if len(mp) >= len(rf.peers)/2 {
		Logger.Printf("[Election] [%v] [T%v] [S%v] receive major vote",
			time.Now().UnixMilli(), rf.Term, rf.me)

		// 投票过程已经重新变为 Follower
		if rf.ServerStatus == ServerStatusFollower {
			Logger.Printf("[Election] [%v] [T%v] [S%v] already become a follower",
				time.Now().UnixMilli(), rf.Term, rf.me)
			return
		}
		res := rf.changeStatus(ServerStatusLeader, rf.me)
		if !res {
			return
		}
	} else {
		Logger.Printf("[Election] [%v] [T%v] [S%v] not receive enough vote", time.Now().UnixMilli(),
			rf.Term, rf.me)
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
func (rf *Raft) changeStatus(toStatus ServerStatus, serverIdx int) bool {
	flag := false
	for _, status := range serverStatusCheck[rf.ServerStatus] {
		if status == toStatus {
			flag = true
		}
	}
	if !flag {
		Logger.Printf("[changeStatus] [%v] [S%v] change server status fail [%v]->[%v]", time.Now().UnixMilli(), rf.me, rf.ServerStatus, toStatus)
		return false
	}
	Logger.Printf("[changeStatus] [%v] [S%v] change server status success [%v]->[%v]", time.Now().UnixMilli(), rf.me, rf.ServerStatus, toStatus)
	rf.ServerStatus = toStatus
	// 这里更新LastHeartBeattime
	// 如果toStatus是Follower，更新是因为收到了leader的心跳
	// 如果toStatus是Candidate, 更新是防止重复发起选举
	// 如果toStatus是Leader, 更新是续约
	rf.LastHeartbeatTime = time.Now().UnixMilli()
	if toStatus == ServerStatusLeader {
		rf.LeaderIdx = serverIdx
		rf.VoteIdx = serverIdx
		rf.LeaderInit()
	}
	if toStatus == ServerStatusFollower {
		rf.LeaderIdx = serverIdx
		rf.VoteIdx = serverIdx
	}
	if toStatus == ServerStatusCandidate {
		rf.LeaderIdx = -1
		rf.VoteIdx = serverIdx
	}

	return true
}

// AppendEntries
/*
// 1. 判断Args.Term Index
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	const (
		AppendEntriesResultSuccess                 = 1
		AppendEntriesResultDenyTermLess            = 2
		AppendEntriesResultDenyTermEqualDiffLeader = 3
		AppendEntriesResultDenyTermEqualNowLeader  = 4
		AppendEntriesResultDenyInvalidPrevLog      = 5
	)

	term := rf.Term
	localLeaderIdx := rf.LeaderIdx
	serverStatus := rf.ServerStatus

	result := util.IntPtr(AppendEntriesResultSuccess)
	defer func() {
		switch *result {
		case AppendEntriesResultSuccess:
			for _, log := range args.Logs {
				rf.ApplyMsgCh <- ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.Idx,
				}
			}
			Logger.Printf("[AppendEntries] [%v] [T%v] [S%v]->[S%v] allow AppendEntries", time.Now().UnixMilli(), args.Term, args.LeaderIdx, rf.me)
		case AppendEntriesResultDenyTermEqualDiffLeader:
			Logger.Printf("[AppendEntries] [%v] [T%v]->[T%v] [S%v]->[S%v] [L%v]->[L%v] deny AppendEntries: leader not equal", time.Now().UnixMilli(),
				args.Term, rf.Term, args.LeaderIdx, rf.me, args.LeaderIdx, localLeaderIdx)
		case AppendEntriesResultDenyTermEqualNowLeader:
			Logger.Printf("[AppendEntries] [%v] [T%v]->[T%v] [S%v]->[S%v] term equal but now is leader",
				time.Now().UnixMilli(), args.Term, rf.Term, args.LeaderIdx, rf.me)
		case AppendEntriesResultDenyTermLess:
			Logger.Printf("[AppendEntries] [%v] [T%v]->[T%v] [S%v]->[S%v] deny AppendEntries: term less than current term", time.Now().UnixMilli(), args.Term, rf.Term, args.LeaderIdx, rf.me)
		default:
		}
	}()
	reply.Term = term
	reply.Success = false
	// term 小于当前term 直接返回
	if args.Term < term {
		result = util.IntPtr(AppendEntriesResultDenyTermLess)
		return
	}

	// Prev Log Term 不合法 当前server不存在PrevLogIdx的Log或者该log term不一致
	if args.PrevLogIdx >= 0 && (args.PrevLogIdx > len(rf.Logs)-1 || rf.Logs[args.PrevLogIdx].Term != args.PrevLogTerm) {
		result = util.IntPtr(AppendEntriesResultDenyInvalidPrevLog)
		return
	}

	// Term相等
	if args.Term == rf.Term {
		// 当前Follower与此次不同，同步失败
		if serverStatus == ServerStatusFollower && rf.LeaderIdx != args.LeaderIdx {
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
	for _, log := range args.Logs {
		for log.Idx > len(rf.Logs)-1 {
			rf.Logs = append(rf.Logs, nil)
		}
		rf.Logs[log.Idx] = log
	}
}

// SendHeartBeat
// 这里也要同步Log
func (rf *Raft) SendHeartBeat() {
	for {
		if rf.killed() {
			Logger.Printf("[HeartBeat] [%v] [S%v] server is killed", time.Now().UnixMilli(), rf.me)
			continue
		}
		time.Sleep(time.Duration(rf.HeartbeatInterval * int(time.Millisecond)))
		rf.mu.Lock()
		status := rf.ServerStatus
		term := rf.Term
		rf.mu.Unlock()
		if status != ServerStatusLeader {
			continue
		}
		replys := make([]*AppendEntriesReply, len(rf.peers))
		wg := sync.WaitGroup{}
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			i := i
			peer := peer
			Logger.Printf("[HeartBeat] [%v] [S%v]->[S%v] SendHeartBeat",
				time.Now().UnixMilli(), rf.me, i)

			args := &AppendEntriesArgs{
				LeaderIdx:   rf.me,
				Term:        term,
				PrevLogIdx:  -1,
				PrevLogTerm: -1,
			}
			reply := &AppendEntriesReply{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := peer.Call("Raft.AppendEntries", args, reply)
				if !ok {
					return
				}
				replys[i] = reply
			}()
		}
		// TODO 这里应该比心跳间隔时间稍微短一点
		util.WaitWithTimeout(&wg, time.Second)
		count := 0
		for _, reply := range replys {
			if reply == nil {
				continue
			}
			if reply.Success {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			Logger.Printf("[HeartBeat] [%v] [S%v] receive major heartbeat", time.Now().UnixMilli(), rf.me)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusLeader, rf.me)
			rf.mu.Unlock()
		} else {
			Logger.Printf("[HeartBeat] [%v] [S%v] not receive major heartbeat", time.Now().UnixMilli(), rf.me)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusCandidate, rf.me)
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term      int
	LeaderIdx int

	PrevLogIdx  int
	PrevLogTerm int
	Logs        []*Log
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

	// Your initialization code here (2A, 2B, 2C).
	// Lab强制每秒限制10次心跳，心跳频率跟选举超时时间无关。
	// 需要保证在一次选举超时时间内可以完整的进行多次心跳即可。
	// 选举超时时间为 400~600ms
	rf.ElectionTimeout = int32(rand.Intn(400) + 200)
	// 初始状态为 follower
	rf.ServerStatus = ServerStatusFollower
	// 每秒 10 次心跳
	rf.HeartbeatInterval = 100 // ms
	// 初始化为-1 未给任何人投票
	rf.VoteIdx = -1
	// 初始化心跳时间
	rf.LastHeartbeatTime = time.Now().UnixMilli()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ApplyMsgCh = applyCh
	Logger.Printf("[Init] [S%v] lastHeartbeatTime:%v", rf.me, rf.LastHeartbeatTime)
	Logger.Printf("[Init] [S%v] electionTimeout:%v", rf.me, rf.ElectionTimeout)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.SendHeartBeat()

	return rf
}
