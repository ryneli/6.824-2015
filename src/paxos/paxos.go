package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

import "time"
// import "reflect"
// import "runtime"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Propose struct {
	mu         sync.Mutex
	index      int
	me         int
	nprepared  int
	naccepted  int
	status     Fate
	value      interface{}
}

type PrepareReq struct {
	Index      int
	Nprepared  int
	Done       int
	Me         int
}

type PrepareReply struct {
	Naccepted  int
	Value      interface{}
	Me         int
	Success    bool
}

type AcceptReq struct {
	Index      int
	Nprepared  int
	Value      interface{}
}

type AcceptReply struct {
	Naccepted  int
	Value      interface{}
	Me         int
	Success    bool
}

type FinishReq struct {
	Index      int
	Value      interface{}
}

type FinishReply struct {
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	// Proposer's data stores in function arguments
	// Accepter's data stores in Propose struct
	dones      []int
	maxseq     int
	proposes   map[int]*Propose// message version
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) initPropose(index int, v interface{}) {
	px.proposes[index] = &Propose{
		mu: sync.Mutex{},
		index: index,
		me: px.me,
		value: v,
		nprepared: 0,
		naccepted: 0,
		status: Pending,
	}

	if index > px.maxseq {
		px.maxseq = index
	}
}

func (px *Paxos) isForget(index int) bool {
	return px.Min() > index
}

func (px *Paxos) HandlePrepare(args PrepareReq, reply * PrepareReply) error {
	i, n := args.Index, args.Nprepared
	if px.isForget(i) {
		reply.Success = false
		return nil
	}
	
	px.mu.Lock()
	px.dones[args.Me] = args.Done
	px.cleanDone()
	
	if _,ok := px.proposes[i]; !ok {
		px.initPropose(i, nil)
	}
	p := px.proposes[i]
	px.mu.Unlock()

	p.mu.Lock()
	if n > p.nprepared {
		p.nprepared = n
		reply.Success = true
	} else {
		reply.Success = false
	}

	reply.Me = px.me
	reply.Naccepted = p.naccepted
	if p.value != nil {
		reply.Value = p.value
	} else {
		reply.Value = nil
	}
	p.mu.Unlock()
	// fmt.Printf("%v:HandlePrepare %v %v\n", px.me, args, reply)
	return nil
}

func (px *Paxos) CallPrepare(server string, index int, nprepared int, ch chan<- PrepareReply) {
	px.mu.Lock()
	args := PrepareReq {
		Index: index,
		Nprepared: nprepared,
		Done: px.dones[px.me],
		Me: px.me,
	}
	px.mu.Unlock()
	reply := PrepareReply{
		Naccepted: 0,
		Value: nil,
		Me: -1,
		Success: false,
	}

	if server != px.peers[px.me] {
		c := make(chan bool)
		go func() {
			call(server, "Paxos.HandlePrepare", args, &reply)
			c <- true
		}()
		select {
		case _ = <-c:
		case <-time.After(time.Second*2):
			// fmt.Printf("%v:CallPrepare timeout\n", px.me)
		}
	} else {
		px.HandlePrepare(args, &reply)
	}
	// fmt.Printf("%v:CallPrepare %v %v\n", px.me, args, reply)
	ch <- reply
}

func (px *Paxos) prepare(seq int, nprepared int) (int, interface{}, bool) {
	ch := make(chan PrepareReply)
	nReply, nSucc, np := 0, 0, 0
	var v interface{}
	for _,s := range px.peers {
		go px.CallPrepare(s, seq, nprepared, ch)
	}

	for reply := range ch {
		nReply++
		if reply.Success {
			nSucc++
		}
		// fmt.Printf("%v:prepare, (%v|%v|%v)%v-%v|%v\n", px.me, len(px.peers), nReply, nSucc, seq, nprepared, reply)
		if np <= reply.Naccepted {
			np = reply.Naccepted
			v = reply.Value
		}
		
		if nSucc > len(px.peers)/2 {
			// fmt.Printf("%v:prepare[succ], (%v|%v|%v)%v\n", px.me, len(px.peers), nReply, nSucc, reply)
			return np+1, v, true
		} else if nReply-nSucc > len(px.peers)/2 {
			// fmt.Printf("%v:prepare[fail], (%v|%v|%v)%v\n", px.me, len(px.peers), nReply, nSucc, reply)
			return np+1, v, false
		}
	}
	return np+1, v, false
}

func (px *Paxos) HandleAccept(args AcceptReq, reply *AcceptReply) error {
	i, n, v := args.Index, args.Nprepared, args.Value
	if px.isForget(i) {
		reply.Success = false
		return nil
	}
	px.mu.Lock()
	if _,ok := px.proposes[i]; !ok {
		px.initPropose(i, nil)
	}
	p := px.proposes[i]
	px.mu.Unlock()

	p.mu.Lock()
	if n >= p.nprepared {
		p.nprepared = n
		p.naccepted = n
		p.value = v
		reply.Success = true
	} else {
		reply.Success = false
	}

	reply.Me = px.me
	reply.Naccepted = p.naccepted
	if p.value != nil {
		reply.Value = p.value
	} else {
		reply.Value = nil
	}
	p.mu.Unlock()
	return nil
}

func (px *Paxos) CallAccept(server string, index int, nprepared int, value interface{}, ch chan<- AcceptReply) {
	args := AcceptReq{
		Index: index,
		Nprepared: nprepared,
		Value: value,
	}
	reply := AcceptReply{
		Naccepted: 0,
		Value: nil,
		Me: -1,
		Success: false,
	}
	if server != px.peers[px.me] {		
		c := make(chan bool)
		go func() {
			call(server, "Paxos.HandleAccept", args, &reply)
			c <- true
		}()
		select {
		case _ = <-c:
		case <-time.After(time.Second*2):
			// fmt.Printf("%v:CallAccept timeout\n", px.me)
		}
	} else {
		px.HandleAccept(args, &reply)
	}
	ch <- reply
}

func (px *Paxos) accept(seq int, nprepared int, v interface{}) (int, interface{}, bool) {
	ch := make(chan AcceptReply)
	nReply, nSucc, np := 0, 0, nprepared
	var nv interface{}
	for _, s := range px.peers {
		go px.CallAccept(s, seq, nprepared, v, ch)
	}

	for reply := range ch {
		nReply++
		if reply.Success {
			nSucc++
		}
		// fmt.Printf("%v:accept, (%v|%v|%v)%v\n", px.me, len(px.peers), nReply, nSucc, reply)
		if np < reply.Naccepted {
			np = reply.Naccepted
			nv = reply.Value
		}

		if nSucc > len(px.peers)/2 {
			return np+1, nv, true
		} else if nReply-nSucc > len(px.peers)/2 {
			return np+1, nv, false
		}
	}
	return np+1, nv, false
}

func (px *Paxos) HandleFinish(req FinishReq, reply * FinishReply) error {
	if px.isForget(req.Index) {
		return nil
	}
	px.mu.Lock()
	if _, ok := px.proposes[req.Index]; !ok {
		px.initPropose(req.Index, nil)
	}
	px.proposes[req.Index].status = Decided
	px.proposes[req.Index].value = req.Value
	px.mu.Unlock()
	// fmt.Printf("%v:HandleFinish %v\n", px.me, reflect.ValueOf(req.Value).Len())
	
	// runtime.GC()
	// var m1 runtime.MemStats
	// runtime.ReadMemStats(&m1)
	// fmt.Printf("%v:HandleFinish[%v] %v:%v\n", px.me, m1.Alloc, req.Index, reply)
	return nil
}

func (px *Paxos) CallFinish(server string, index int, value interface{}) {
	args := FinishReq{
		Index: index,
		Value: value,
	}
	reply := FinishReply{}
	// fmt.Printf("%v:CallFinish %v|%v\n", px.me, args, reply)
	if server != px.peers[px.me] {
		call(server, "Paxos.HandleFinish", args, &reply)
	} else {
		px.HandleFinish(args, &reply)
	}
}

func (px *Paxos) finish(seq int, value interface{}) {
	// fmt.Printf("%v:finish seq(%v)\n", px.me, seq)
	for _,s := range px.peers {
		go px.CallFinish(s, seq, value)
	}
}

func (px *Paxos) doPaxos(seq int, v interface{}) {
	// init propse struct
	// fmt.Printf("%v:doPaxos %v|%v\n", px.me, seq, v)
	px.mu.Lock()
	if _,ok := px.proposes[seq]; !ok {
		if px.min() <= seq {
			px.initPropose(seq, v)
		} else {
			px.mu.Unlock()
			return
		}
	} else {
		px.mu.Unlock()
		return
	}
	p := px.proposes[seq]
	px.mu.Unlock()

	np := 1
	value := v
	retry := 0
	for !px.isdead() && p.status != Decided {
		// fmt.Printf("%v:doPaxos proposal %v retry %v times\n", px.me, p, retry)
		retry++
		// random start
		x := rand.Intn(10)
		time.Sleep(time.Duration(x*10) * time.Millisecond)
		
		if n, nv, ok := px.prepare(seq, np); !ok {
			if np < n && nv != nil {
				np = n
				value = nv
			} else {
				np++
			}
			continue
		} else if nv != nil {
			value = nv
		}
		// fmt.Printf("%v:doPaxos prepare pass %v|%v\n", px.me, np, reflect.ValueOf(value).Len());
		
		if n, nv, ok := px.accept(seq, np, value); !ok {
			if np < n && nv != nil {
				np = n
				value = nv
			} else {
				np++
			}
			continue
		}
		// fmt.Printf("%v:doPaxos accept pass %v|%v\n", px.me, np, reflect.ValueOf(value).Len());
		
		px.finish(seq, value)
		break
	}
	// fmt.Printf("%v:doPaxos %v|%v return\n", px.me, px.isdead(), p.status)
}

func (px *Paxos) printProposals() {
	fmt.Printf("%v Proposal List\n", px.me)
	for k, _ := range px.proposes {
		fmt.Printf("\t%v\n", k)
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	// fmt.Printf("%v:Start seq(%v)v(%v)\n", px.me, seq, reflect.ValueOf(v).Len())
	go px.doPaxos(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	px.dones[px.me] = seq
	px.cleanDone()
	// fmt.Printf("%v:Done %v\n", px.me, px.dones)
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxseq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)
func (px *Paxos) cleanDone() {
	mindone := MaxInt
	for i := 0; i < len(px.dones); i++ {
		if mindone > px.dones[i] {
			mindone = px.dones[i]
		}
	}
	for k, _ := range px.proposes {
		if mindone >= k {
			delete(px.proposes, k)
			// fmt.Printf("%v:cleanDone %v|%v|%v\n", px.me, k, mindone, len(px.proposes))
		}
	}
}

func (px *Paxos) min() int {
	// You code here.
	mindone := MaxInt
	for i := 0; i < len(px.dones); i++ {
		if mindone > px.dones[i] {
			mindone = px.dones[i]
		}
	}
	return mindone+1
}

func (px *Paxos) Min() int {
	px.mu.Lock()
	res := px.min()
	px.mu.Unlock()
	return res
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	status := Pending
	var value interface{}
	px.mu.Lock()
	if p, ok := px.proposes[seq]; ok {
		status, value = p.status, p.value
	} else {
		status, value = Forgotten, nil
	}
	px.mu.Unlock()
	// fmt.Printf("%v:Status %v|%v\n", px.me, seq, status)
	return status, value
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.dones    = make([]int, len(px.peers))
	for i := 0; i < len(px.dones); i++ {
		px.dones[i] = -1
	}
	px.maxseq = 0
	px.proposes = make(map[int]*Propose)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
