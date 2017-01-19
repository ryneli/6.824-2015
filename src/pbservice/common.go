package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string
	Msgnum   int64
	Viewnum  uint
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Msgnum   int64
	Viewnum  uint
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ExportDBArgs struct {
	Me       string
	Viewnum  uint
}

type ExportDBReply struct {
	Kv       map[string]string
	Msgbuf   map[int64]string
}
