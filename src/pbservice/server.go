package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	//pendingbuf map[int64]string // NOTE: it seems no pending msg, just return immidiately
	msgbuf     map[int64]string   // buffer results
	view       viewservice.View   // current view
	kvViewnum  int                // [mutex] the Viewnum of latest kv update    
	kv         map[string]string  // key-value DB
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	v,ok := pb.kv[args.Key]

	if ok {
		reply.Err = OK
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
	// fmt.Printf("GET (%s:%s)\n", args.Key, reply.Value)
	// TODO: pass msg to Backup
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	// fmt.Printf("PutAppend %s %s %s\n", args.Op, args.Key, args.Value)
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}
	if _,ok := pb.kv[args.Key]; ok && args.Op == "Append" {
		pb.kv[args.Key] += args.Value
	} else {
		pb.kv[args.Key] = args.Value
	}
	// fmt.Printf("PUT (%s:%s)\n", args.Key, args.Value)
	// TODO: pass msg to Backup
	reply.Err = OK
	return nil
}

func (pb *PBServer) ExportDB(args *ExportDBArgs, reply *ExportDBReply) error {
	// TODO: check if Backup call me
	reply.kv = pb.kv
	reply.kvViewnum = pb.view.Viewnum
	return nil
}

func (pb *PBServer) tryImportDB() {
	if pb.kvViewnum == pb.view.Viewnum {
		return
	}
	args := ExportDBArgs{Me:pb.me, Viewnum:pb.view.Viewnum}
	var reply ExportDBReply
	ok = false
	for !ok {
		ok = call(pb.view.Primary, "PBServer.ExportDB", args, &reply)
	}
	pb.kv = reply.kv
	// put update kvViewnum at last
	pb.kvViewnum = reply.kvViewnum
}

func (pb *PBServer) isPrimary() bool {
	return !pb.isdead() && pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return !pb.isdead() && pb.view.Backup == pb.me
}
//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	// if isdead, dont ping Viewserver
	if pb.isdead() {
		return
	}
	// update view
	if newView, err := pb.vs.Ping(pb.view.Viewnum); err == nil {
		pb.view = newView
	}
	// fmt.Printf("%s %s %d\n", pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
	// Backup server need to tell Primary server if B need database information
	if pb.isBackup() {
		pb.tryImportDB()
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{Primary:"", Backup:"", Viewnum:0}
	pb.kv = make(map[string]string)
	pb.msgbuf = make(map[int64]string)
	pb.kvViewnum = 0
	
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
