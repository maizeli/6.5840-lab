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
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

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
	// Logger.SetOutput(io.Discard)
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

	// For Debug
	Pos string
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
	Logs         []*Log
	NextIdxMap   sync.Map // 这里每次当选leader时应该被置为当前leader的值？
	ApplyMsgCh   chan ApplyMsg
	CommittedIdx int        // 已经提交的Idx
	StartMu      sync.Mutex // 用于保证Start的串行性
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
	Logger.Printf("[LeaderInit] [S%v] [T%v] start...", rf.me, rf.Term)
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIdxMap.Store(i, len(rf.Logs))
	}
	Logger.Printf("[LeaderInit] [S%v] [T%v] end...", rf.me, rf.Term)
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
	Logger.Printf("[RequestVote] [S%v] [T%v] recv [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, rf.me)
	defer func() {
		if reply.IsVote {
			Logger.Printf("[RequestVote] [S%v] [T%v] vote for [S%v] [T%v] RequestVote", rf.me, rf.Term, args.ServerIdx, rf.Term)
		} else {
			Logger.Printf("[RequestVote] [S%v] [T%v] deny [S%v] [T%v] RequestVote:%v", rf.me, rf.Term, args.ServerIdx, rf.me, reply.DenyReason)
		}
	}()
	reply.Term = rf.Term
	reply.ServerIdx = rf.me
	// 1. 过期Term
	if args.CurrentTerm < rf.Term {
		reply.DenyReason = fmt.Sprintf("term less")
		return
	}

	if args.CurrentTerm > rf.Term {
		rf.Term = args.CurrentTerm
		rf.changeStatus(ServerStatusFollower, -1, false) // 这里先变为Follower，但是不更新上次心跳时间，只有为此Server投票时才能更新心跳时间。 防止当前server是唯一一个能当选的的server，由于被其他server频繁的"打扰",无法发起选举
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

	if args.CurrentTerm == rf.Term {
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 加锁保证只有一个Start在运行
	rf.StartMu.Lock()
	defer rf.StartMu.Unlock()

	Logger.Printf("[Start] [S%v] [T%v] command = %v", rf.me, rf.Term, util.JSONMarshal(command))

	idx := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.ServerStatus == ServerStatusLeader
	// 非Leader直接返回
	if !isLeader {
		Logger.Printf("[Start] [S%v] [T%v] is not leader", rf.me, rf.Term)
		rf.mu.Unlock()
		return idx, term, isLeader
	}
	// 当前命令Idx=CommittedIdx+1
	committedIdx := rf.CommittedIdx
	idx = rf.CommittedIdx + 1
	if idx >= len(rf.Logs) {
		rf.Logs = append(rf.Logs, nil)
	}
	// 记录当前Term
	term = rf.Term
	// 将command添加到leader本地状态机
	rf.Logs[idx] = &Log{
		Term:    term,
		Idx:     idx,
		Command: command,
	}
	// 记录当前Log
	logs := rf.Logs
	rf.mu.Unlock()

	wg := &sync.WaitGroup{}
	replys := make([]*AppendEntriesReply, len(rf.peers))
	// Leader开始复制新Log
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
				if rf.killed() {
					return
				}
				// 获取对应Follower该从哪一条Log开始复制
				prevLogIdx, prevLogTerm := -1, -1
				if nextIdxAny, ok := rf.NextIdxMap.Load(i); ok {
					prevLogIdx = nextIdxAny.(int) - 1
				}
				if len(logs) > 0 && prevLogIdx >= 0 {
					prevLogTerm = logs[prevLogIdx].Term
				}
				args := &AppendEntriesArgs{
					Term:            term,
					LeaderIdx:       rf.me,
					PrevLogIdx:      prevLogIdx,
					PrevLogTerm:     prevLogTerm,
					LeaderCommitIdx: committedIdx,
				}
				if prevLogIdx+1 >= 0 {
					args.Logs = logs[prevLogIdx+1:]
				}
				Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] args=%v", rf.me, term, i, util.JSONMarshal(args))
				reply := &AppendEntriesReply{}
				res := peer.Call("Raft.AppendEntries", args, reply)
				if !res {
					Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v], res=false", rf.me, term, i)
					return
				}
				replys[i] = reply
				// 如果出现并发更新可能会导致leader丢失一部分数据，但是问题不大只是增加同步的时间
				if reply.Success {
					atomic.AddInt32(&cnt, 1)
					rf.NextIdxMap.Store(i, len(logs))
					Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] success", rf.me, term, i)
					break
				} else {
					// 这里失败了可能是term不合法
					if reply.Term > term {
						return
					}
					// 因为前一条Log不匹配，NextLogIdx-1，再试下
					Logger.Printf("[Start] [S%v] [T%v] send AppendEntries to [S%v] fail, log idx -1 and retry", rf.me, term, i)
					rf.NextIdxMap.Store(i, prevLogIdx-1)
					if prevLogIdx == -1 {
						Logger.Printf("[Start] [S%v] [T%v] prev log idx is -1, break", rf.me, term)
						break
					}
					prevLogIdx--
				}
			}
		}()
	}
	// TODO 超时时长待确定
	timeOut := util.WaitWithTimeout(wg, time.Duration(rf.ElectionTimeout)*time.Millisecond)
	if timeOut {
		Logger.Printf("[Start] [S%v] [T%v] time out", rf.me, term)
	} else {
		Logger.Printf("[Start] [S%v] start successful", rf.me)
	}
	maxTerm := term
	for _, reply := range replys {
		if reply == nil {
			continue
		}
		maxTerm = util.Max(maxTerm, reply.Term)
	}
	if maxTerm > term {
		rf.mu.Lock()
		if maxTerm > rf.Term {
			rf.changeStatus(ServerStatusFollower, -1)
			rf.Term = maxTerm
		}
		rf.mu.Unlock()
		return idx + 1, maxTerm, true // 这里+1是因为测试认为idx从1开始
	}
	if int(cnt) < len(rf.peers)/2 {
		Logger.Printf("[Start] [S%v] [T%v] less than half server aggree command, cnt(%v) %v", rf.me, term, cnt, command)
	} else {
		Logger.Printf("[Start] [S%v] [T%v] more than half server aggree command cnt(%v) %v", rf.me, term, cnt, command)
		rf.mu.Lock()
		rf.CommitIdx(idx)
		rf.mu.Unlock()
	}

	return idx + 1, term, true
}

// CommitIdx 执行CommitIdx需在外层获取rf.mu
func (rf *Raft) CommitIdx(idx int) {
	if idx <= rf.CommittedIdx {
		Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v <= rf.CommitedIdx %v, not need commit", rf.me, rf.Term, idx, rf.CommittedIdx)
		return
	}
	if idx >= len(rf.Logs) {
		Logger.Printf("[CommitIdx] [S%v] [T%v] commit idx %v > len(rf.Logs) %v, not need commit", rf.me, rf.Term, idx, len(rf.Logs))
		return
	}
	Logger.Printf("[CommitIdx] [S%v] [T%v] begin commit idx %v", rf.me, rf.Term, idx)
	for i := rf.CommittedIdx + 1; i <= idx; i++ {
		// Logger.Printf("[CommitIdx] [S%v] [T%v] send apply msg:%v", rf.me, rf.Term, i+1)
		rf.ApplyMsgCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[i].Command,
			CommandIndex: i + 1,
			Pos:          util.TernaryOperate(len(rf.Logs) == 0, "HeartBeat", "Start"),
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
	Logger.Printf("[Kill] [S%v] is killed", rf.me)
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
	Logger.Printf("[ticker] [S%v] stop listen heart beat timeout", rf.me)
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
	Logger.Printf("[Election] [S%v] [T%v] start election", rf.me, rf.Term)
	// 1. 给自己投一票 这里应该不会失败
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
				CurrentTerm: localTerm,
				ServerIdx:   rf.me,
			}
			if lastCommand != nil {
				args.LastLogIdx = lastCommand.Idx
				args.LastLogTerm = lastCommand.Term
			}
			reply := &RequestVoteReply{}
			Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote...", rf.me, localTerm, i)
			res := rf.sendRequestVote(i, args, reply)
			if !res {
				Logger.Printf("[Election] [S%v] [T%v] -> [S%v] send RequestVote fail", rf.me, localTerm, i)
				return
			}
			replyChan <- reply
		}()
	}
	Logger.Printf("[Election] [S%v] [T%v] send all RequestVote success, begin waiting....", rf.me, localTerm)
	// 这里不能傻傻等待所有服务器响应，只要超过一半的server投票，就应该停止
	voteCount, flag, tCh := 0, false, time.After(time.Millisecond*time.Duration(rf.ElectionTimeout)*1/2)
	for {
		if flag {
			break
		}
		select {
		case reply := <-replyChan:
			// Logger.Printf("[Election] [S%v] recv [s%v] RequestVoteReply", rf.me, reply.ServerIdx)
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
	var maxTerm int
	for _, reply := range replys {
		if reply == nil {
			Logger.Printf("[Election] [S%v] invalid reply", rf.me)
			continue
		}
		if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		if reply.IsVote {
			Logger.Printf("[Election] [S%v] receive vote from [S%v]", rf.me, reply.ServerIdx)
		} else {
			Logger.Printf("[Election] [S%v] NOT receive vote from [S%v]", rf.me, reply.ServerIdx)
		}
	}
	// 4. 统计结果
	/*
		中间可能有意外发生:
		1. 选举过程中收到其他 leader 的心跳，此时变为Follower
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 此时term已经过期 已经变成其他人的小弟or已经开始下一轮选举
	if rf.Term != localTerm {
		Logger.Printf("[Election] [S%v] [T%v] term is not equal, current term is [%v]", rf.me, rf.Term, localTerm)
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
		Logger.Printf("[Election] [S%v] [T%v] already become a follower", rf.me, rf.Term)
		return
	}
	// 包含等于:因为自己会给自己投一票
	if voteCount >= len(rf.peers)/2 {
		Logger.Printf("[Election] [S%v] [T%v] receive major vote[%v]", rf.me, rf.Term, voteCount)
		_ = rf.changeStatus(ServerStatusLeader, rf.me)
	} else {
		Logger.Printf("[Election] [S%v] [T%v] not receive enough vote", rf.me, rf.Term)
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
		Logger.Printf("[changeStatus] [S%v] change server status fail [%v]->[%v]", rf.me, rf.ServerStatus, toStatus)
		return false
	}
	Logger.Printf("[changeStatus] [S%v] change server status [%v]->[%v]", rf.me, rf.ServerStatus, toStatus)
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
		rf.LeaderIdx = serverIdx
		rf.VoteIdx = serverIdx
		// 只有之前不是Leader再执行即可
		if oldStatus != ServerStatusLeader {
			rf.LeaderInit()
		}
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
			Logger.Printf("[AppendEntries] [T%v] [S%v]->[S%v] allow AppendEntries", args.Term, args.LeaderIdx, rf.me)
		case AppendEntriesResultDenyTermEqualDiffLeader:
			Logger.Printf("[AppendEntries] [T%v]->[T%v] [S%v]->[S%v] [L%v]->[L%v] deny AppendEntries: leader not equal",
				args.Term, rf.Term, args.LeaderIdx, rf.me, args.LeaderIdx, localLeaderIdx)
		case AppendEntriesResultDenyTermEqualNowLeader:
			Logger.Printf("[AppendEntries] [T%v]->[T%v] [S%v]->[S%v] term equal but now is leader",
				args.Term, rf.Term, args.LeaderIdx, rf.me)
		case AppendEntriesResultDenyTermLess:
			Logger.Printf("[AppendEntries] [T%v]->[T%v] [S%v]->[S%v] deny AppendEntries: term less than current term", args.Term, rf.Term, args.LeaderIdx, rf.me)
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
		if serverStatus == ServerStatusFollower && rf.LeaderIdx != -1 && rf.LeaderIdx != args.LeaderIdx {
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
	rf.CommitIdx(args.LeaderCommitIdx)
}

// SendHeartBeat
// TODO 发送心跳的时候也要携带 prevLogIdx prevLogTerm，因为收到消息的server会提交日志，所以需要通过prevLogIdx/Term保证日志的一致
func (rf *Raft) SendHeartBeat() {
	debugIdx := 0
	for rf.killed() == false {
		debugIdx++
		time.Sleep(time.Duration(rf.HeartbeatInterval * int(time.Millisecond)))
		rf.mu.Lock()
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
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			i := i
			peer := peer

			prevLogIdx, prevLogTerm := -1, -1
			if nextIdxAny, ok := rf.NextIdxMap.Load(i); ok {
				prevLogIdx = nextIdxAny.(int) - 1
			}
			if len(logs) > 0 && prevLogIdx >= 0 {
				prevLogTerm = logs[prevLogIdx].Term
			}

			args := &AppendEntriesArgs{
				LeaderIdx:       rf.me,
				Term:            term,
				PrevLogIdx:      prevLogIdx,
				PrevLogTerm:     prevLogTerm,
				LeaderCommitIdx: committedIdx,
			}

			Logger.Printf("[HeartBeat] [S%v]->[S%v] debugIDx=%v args=%v", rf.me, i, debugIdx, util.JSONMarshal(args))
			reply := &AppendEntriesReply{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := peer.Call("Raft.AppendEntries", args, reply)
				if !ok {
					return
				}
				replyMutx.Lock()
				replys[i] = reply
				replyMutx.Unlock()
			}()
		}
		// TODO 这里收到大多数的回应就可以继续保持leader了，不需要等待超时
		util.WaitWithTimeout(&wg, time.Duration(rf.HeartbeatInterval*int(time.Millisecond))*4/5)
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
			Logger.Printf("[HeartBeat] [S%v] debugIdx=%v receive major heartbeat", rf.me, debugIdx)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusLeader, rf.me)
			rf.mu.Unlock()
		} else {
			Logger.Printf("[HeartBeat] [S%v] debugIdx=%v not receive major heartbeat", rf.me, debugIdx)
			rf.mu.Lock()
			rf.changeStatus(ServerStatusCandidate, rf.me)
			rf.mu.Unlock()
			// 这里是否立刻发起选举
		}
	}
	Logger.Printf("[SendHeartBeat] [S%v] stop send heart beat", rf.me)
}

type AppendEntriesArgs struct {
	Term      int
	LeaderIdx int

	PrevLogIdx      int
	PrevLogTerm     int
	Logs            []*Log
	LeaderCommitIdx int
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
	rf.ElectionTimeout = int32(rand.Intn(200) + 400)
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
	rf.CommittedIdx = -1
	Logger.Printf("[Init] [S%v] lastHeartbeatTime:%v", rf.me, rf.LastHeartbeatTime)
	Logger.Printf("[Init] [S%v] electionTimeout:%v", rf.me, rf.ElectionTimeout)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.SendHeartBeat()

	return rf
}
