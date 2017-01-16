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
	// If Backup update first, it will get DB from Primary with lock of server
	// If Primary update first, it will update DB first. So in every ease, it is fine to dump DB, than use it directly.
	view       viewservice.View   // [mutex] current view
	isUpdate   bool               // if export DB to Backup
	kv         map[string]string  // [mutex] key-value DB
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	for i := 0; i < 1; i++ {
		if !pb.isPrimary() {
			reply.Err = ErrWrongServer
			break
		}
		
		// critical region
		v,ok := pb.kv[args.Key]
		
		if ok {
			reply.Err = OK
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
		}
	}
	pb.mu.Unlock()
	// fmt.Printf("[PBServer.Get:%s] (%s:%s:%s)\n", pb.me, args.Key, reply.Value, reply.Err)

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	if !pb.isPrimary() && !pb.isBackup() {
		reply.Err = ErrWrongServer
		return nil
	}

	reply.Err = OK
	// critical region
	ok := true
	for {
		// repeat message
		if _,ok:=pb.msgbuf[args.Msgnum]; ok {
			// fmt.Printf("PutAppend repeat %s %s %s %d %s\n", args.Op, args.Key, args.Value, args.Msgnum, reply.Err)
			break
		}
		
		if pb.isPrimary() && pb.view.Backup != "" && pb.isUpdate == true {
			ok = call(pb.view.Backup, "PBServer.PutAppend", args, reply)
		}
		// fmt.Printf("[PutAppend] %v %v\n", ok, reply.Err)
		if ok && reply.Err != ErrWrongServer {
			if _,ok = pb.kv[args.Key]; ok && args.Op == "Append" {
				pb.kv[args.Key] += args.Value
				pb.msgbuf[args.Msgnum] = ""
			} else {
				pb.kv[args.Key] = args.Value
				pb.msgbuf[args.Msgnum] = ""
			}
			break
		} else {
			// update view
			if newView, err := pb.vs.Ping(pb.view.Viewnum); err == nil {
				if pb.view.Primary != newView.Primary {
					pb.isUpdate = false
				}
				pb.view = newView
			}
			// fmt.Printf("[PutAppend] update view %s %s %d\n", pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
		}
	}
	pb.mu.Unlock()
	
	return nil
}

func (pb *PBServer) ExportDB(args *ExportDBArgs, reply *ExportDBReply) error {
	// TODO: check if Backup call me
	pb.mu.Lock()
	// Map copy is tricky
	reply.Kv = make(map[string]string)
	for k,v:= range pb.kv {
		reply.Kv[k] = v
	}
	reply.Msgbuf = make(map[int64]string)
	for k,v:= range pb.msgbuf {
		reply.Msgbuf[k] = v
	}
	pb.mu.Unlock()
	pb.isUpdate = true
	return nil
}

func (pb *PBServer) tryImportDB() {
	if pb.isUpdate {
		return
	}
	args := ExportDBArgs{Me:pb.me, Viewnum:pb.view.Viewnum}
	var reply ExportDBReply
	ok := false
	for !ok {
		ok = call(pb.view.Primary, "PBServer.ExportDB", args, &reply)
	}
	pb.mu.Lock()
	for k,v:= range reply.Kv {
		pb.kv[k] = v
	}
	for k,v:= range reply.Msgbuf {
		pb.msgbuf[k] = v
	}
	pb.mu.Unlock()
	pb.isUpdate = true
}

// TODO: Could Primary server know primary change in time?
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
		pb.kv = make(map[string]string)
		pb.msgbuf = make(map[int64]string)
		return
	}
	// update view
	if newView, err := pb.vs.Ping(pb.view.Viewnum); err == nil {
		if pb.view.Primary != newView.Primary {
			pb.isUpdate = false
		}
		pb.view = newView
	}
	// fmt.Printf("[tick]%s %s %d\n", pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
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
	pb.isUpdate = false
	
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
