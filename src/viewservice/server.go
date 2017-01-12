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
	views    []View           // [MUTEX] buffered View
	servers  map[string]time.Time   // [MUTEX] servername : timeout times
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	incViewnum := uint(1)
	if len(vs.views) > 0 {
		incViewnum = vs.views[0].Viewnum+1
	}
	vs.mu.Lock()
	// 1. Try to remove disconnect server in latest view and update server ping
	if len(vs.views) > 0 {
		if _, ok := vs.servers[vs.views[0].Primary]; !ok {
			vs.views[0].Primary = ""
		} else if vs.views[0].Primary == args.Me && args.Viewnum == 0 {
			vs.views[0].Primary = ""
		}
		if _, ok := vs.servers[vs.views[0].Backup]; !ok {
			vs.views[0].Backup = ""
		}
		if vs.views[0].Primary == "" && vs.views[0].Backup != "" {
			// vs.views[0].Primary, vs.views[0].Backup = vs.views[0].Backup, vs.views[0].Primary
			v := View{Primary:vs.views[0].Backup, Backup:"", Viewnum:incViewnum}
			vs.views = append([]View{v}, vs.views...)
		}
	}
	vs.servers[args.Me] = time.Now()
	vs.mu.Unlock()
	// 2. try to update views
	// 2.1 initialization
	if len(vs.views) == 0 || (vs.views[0].Primary != args.Me && vs.views[0].Backup != args.Me) {
		if len(vs.views) == 0 {
			vs.views = append([]View{{incViewnum, args.Me, ""}}, vs.views...)
			// 2.2 set Primary
		} else if vs.views[0].Primary == "" && vs.views[0].Backup == "" {
			vs.views = append([]View{{incViewnum, args.Me, ""}}, vs.views...)
			// 2.3 promote Backup
		} else if vs.views[0].Primary == "" && vs.views[0].Backup != "" {
			latest := vs.views[0]
			vs.views = append([]View{{incViewnum, latest.Backup, args.Me}}, vs.views...)	
			// 2.4 set Backup
		} else if vs.views[0].Backup == "" {
			latest := vs.views[0]
			vs.views = append([]View{{incViewnum, latest.Primary, args.Me}}, vs.views...)
		}
	}
	fmt.Printf("ping %s %d (%s %s %d)\n", args.Me, args.Viewnum, vs.views[0].Primary, vs.views[0].Backup, vs.views[0].Viewnum)
	// note: len(views) should be larger than 0
	// assert.NotEqual(len(vs.views), 0)
	// 3.  delete all viewnum < args.viewnum if the ping is from Primary
	if vs.views[0].Primary == args.Me {
		for vs.views[len(vs.views)-1].Viewnum < args.Viewnum {
			vs.views = vs.views[:len(vs.views)-1]
		}
	}
	// 4.   set reply.View = View[-1]
	reply.View = vs.views[len(vs.views)-1]
	// fmt.Printf("Current servers\n");
	// for k,v := range vs.servers {
	// 	fmt.Printf("\t%s : %d\n", k, v)
	// }
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// 1. set reply.View
	vs.mu.Lock()
	if len(vs.views) > 0 {
		reply.View = vs.views[0]
	}
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
			fmt.Printf("delete %s %d %d\n", k, time.Since(v), DeadPings * PingInterval)
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
