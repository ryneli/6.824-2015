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

	// [MUTEX]
	msgbuf     map[int64]string   // buffer results
	view       viewservice.View   // current view
	kv         map[string]string  // key-value DB
	backupUpdate bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// fmt.Printf("[Get] in\n")
	pb.mu.Lock()
	for i:=0;i< 1;i++ {
		if args.Viewnum != pb.view.Viewnum ||
			(!pb.isPrimary() && !pb.isBackup()) ||
			(pb.view.Backup != "" && !pb.backupUpdate){
			// fmt.Printf("[Get] %d %s %s %d\n", args.Viewnum, pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
			reply.Err = ErrWrongServer
			break
		}

		ok := true
		if pb.isPrimary() && pb.view.Backup != "" && pb.backupUpdate {
			ok = call(pb.view.Backup, "PBServer.Get", args, reply)
			if !ok || reply.Err == ErrWrongServer {
				// fmt.Printf("[PBServer.Get] (%s,%s,%d) Backup error\n", pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
				reply.Err = ErrWrongServer
				break
			}
		}
		v,ok := pb.kv[args.Key]
		
		if ok {
			reply.Err = OK
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
		}
		break
	}
	pb.mu.Unlock()
	// fmt.Printf("[PBServer.Get:%s] (%s:%s:%s)\n", pb.me, args.Key, reply.Value, reply.Err)

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	for i:=0;i< 1;i++ {
		if args.Viewnum != pb.view.Viewnum ||             // old view
		   (!pb.isPrimary() && !pb.isBackup()) ||         // not primary or backup
		   (pb.view.Backup != "" && !pb.backupUpdate) {// backup not ready
			// fmt.Printf("[PutAppend] wrong server %d %s %s %d\n", args.Viewnum, pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
			reply.Err = ErrWrongServer
			break
		}
		
		reply.Err = OK

		// repeat message
		if _,ok:=pb.msgbuf[args.Msgnum]; ok {
			// fmt.Printf("[PutAppend] repeat %s %s %s %d %s\n", args.Op, args.Key, args.Value, args.Msgnum, reply.Err)
			break
		}

		ok := true
		if pb.isPrimary() && pb.view.Backup != "" && pb.backupUpdate {
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
			reply.Err = ErrWrongServer
			break
		}
	}
	pb.mu.Unlock()
	// fmt.Printf("[PutAppend] %s %d (%s,%s,%d,%v) reply.Err %s (%s,%s)\n",
	//	pb.me, args.Viewnum,
	//	pb.view.Primary, pb.view.Backup, pb.view.Viewnum, pb.backupUpdate,
	//	reply.Err,
	//	args.Key, args.Value,
	//)
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
	pb.backupUpdate = true
	pb.mu.Unlock()
	// fmt.Printf("ExportDB \n");
	return nil
}

func (pb *PBServer) tryImportDB() {
	if pb.backupUpdate {
		return
	}
	args := ExportDBArgs{Me:pb.me, Viewnum:pb.view.Viewnum}
	var reply ExportDBReply
	ok := call(pb.view.Primary, "PBServer.ExportDB", args, &reply)
	if ok {
		for k,v:= range reply.Kv {
			pb.kv[k] = v
		}
		for k,v:= range reply.Msgbuf {
			pb.msgbuf[k] = v
		}
		pb.backupUpdate = true
	} else {
		// fmt.Printf("ImportDB fail %v %v\n", pb.view.Primary, ok);
	}
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
	pb.mu.Lock()
	// if isdead, dont ping Viewserver
	if pb.isdead() {
		pb.kv = make(map[string]string)
		pb.msgbuf = make(map[int64]string)
		pb.backupUpdate = false
		pb.view = viewservice.View{Primary:"", Backup:"", Viewnum:0}
	} else {
		if pb.isBackup() && pb.backupUpdate == false {
			pb.tryImportDB()
		} else {
			newView, err := pb.vs.Ping(pb.view.Viewnum)
			if err == nil && pb.view.Viewnum != newView.Viewnum {
				pb.view = newView
				if pb.isBackup() {
					pb.tryImportDB()
				}
			}
		}
	}

	pb.mu.Unlock()
	// fmt.Printf("[tick]%s %s %s %d\n", pb.me, pb.view.Primary, pb.view.Backup, pb.view.Viewnum)
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
	pb.backupUpdate = false
	
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
