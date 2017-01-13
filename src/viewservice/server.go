package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	view       View
	confirmed  bool                 // Primary server confirmed, view can ONLY be modified when confirmed
	servers  map[string]time.Time   // [MUTEX] servername : timeout times
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	// 1. check confirmed view. ONLY confirmed view can be changed
	if vs.view.Primary == args.Me && vs.view.Viewnum == args.Viewnum {
		vs.confirmed = true
	}
	// 2. set next Viewnum. Sometimes Primary server will rollback Viewnum, WHY?
	nextViewnum := vs.view.Viewnum+1
	if (vs.view.Primary == args.Me) && (args.Viewnum < vs.view.Viewnum) {
		nextViewnum = args.Viewnum+1
	}
	vs.mu.Lock()
	// 3. update servers map
	vs.servers[args.Me] = time.Now()
	// 4. if confirmed, update view
	if vs.confirmed {
		// 4.1.1 reset Primary server if timeout
		if _,ok := vs.servers[vs.view.Primary]; !ok {
			vs.view.Primary = ""
		// 4.1.2 reset Backup server if timeout, ONLY reset Primary or Backup at one time
		} else if _,ok := vs.servers[vs.view.Backup]; !ok {
			vs.view.Backup = ""
		}
		
		// 4.2.1 Primary server reset, use Backup server as Primary server
		if vs.view.Primary == args.Me && args.Viewnum == 0 && vs.view.Backup != "" {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			vs.view.Viewnum = nextViewnum
			vs.confirmed = false
		// 4.2.2 Primary server timeout, use Backup server as Primary server
		} else if vs.view.Primary == "" && vs.view.Backup != "" {
			// fmt.Printf("%s %s %d %d %d\n", vs.view.Primary, args.Me, args.Viewnum, vs.view.Viewnum, nextViewnum)
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			vs.view.Viewnum = nextViewnum
			vs.confirmed = false
		// 4.2.3 Primary server rollback. It is considered as CONFIRMED
		} else if vs.view.Primary == args.Me && nextViewnum < vs.view.Viewnum {
			vs.view.Viewnum = nextViewnum
			vs.confirmed = true // confirm a smaller Viewnum
		}

		// 4.3.1 use args.Me as Primary server if no Primary
		if vs.view.Primary == "" {
			vs.view.Primary = args.Me
			vs.view.Viewnum = nextViewnum
			vs.confirmed = false
		// 4.3.2 use args.Me as Backup server if no Backup
		} else if vs.view.Primary != args.Me && vs.view.Backup == "" {
			vs.view.Backup = args.Me
			vs.view.Viewnum = nextViewnum
			vs.confirmed = false
		}
	}
	vs.mu.Unlock()

	// 5. set reply
	reply.View = vs.view
	// fmt.Printf("ping %s %d P[%s]B[%s]n[%d]\n", args.Me, args.Viewnum, vs.view.Primary, vs.view.Backup, vs.view.Viewnum)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// 1. set reply.View
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	for k, v := range vs.servers {
		if time.Since(v) > DeadPings * PingInterval {
			// fmt.Printf("delete %s %d %d\n", k, time.Since(v), DeadPings * PingInterval)
			delete(vs.servers, k)
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.servers = make(map[string]time.Time)
	vs.confirmed = true
	vs.view = View{Primary:"", Backup:"", Viewnum:0}
	
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
