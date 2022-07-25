package mr

type Stage int
type TaskType int
type TaskStatus int
type WorkerId string

const (
	_ Stage = iota
	MPSTG
	RDCSTG
)

const (
	_ TaskType = iota
	MAP
	REDUCE
)

const (
	_ TaskStatus = iota
	IDLE
	IN_PROGRESS
	COMPLETED
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
