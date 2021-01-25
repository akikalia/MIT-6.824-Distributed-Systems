package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type SynchedVar struct{
	sync.Mutex
	Val int
}

type TaskData struct{
	
	TaskFiles []string
	Start time.Time 
	// Status string
}


type SynchedMap struct{
	sync.Mutex
	Map map[int] *TaskData
}


type SynchedStack struct{
	sync.Mutex
	Stack [][]string
}

type SynchedFlag struct{
	sync.Mutex
	Flag bool
}

type Master struct {
	MapDone SynchedFlag// Your definitions here.
	DoneFlag SynchedFlag // maybe check runing tasks . size() == 0 ?
	NReduce int
	MapTasksRunning SynchedMap
	ReduceTasksRunning SynchedMap
	MapTasks SynchedStack
	ReduceTasks SynchedStack
	TaskID SynchedVar
	NumWorkers SynchedVar
	
	// clock int 
	// RunningTasks map[int]int // or worker id to task Metadata() for worker bookkeeping 
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (m *Master) SendMetadata(args *ExampleArgs, data * Metadata) error {
	data.NReduce = m.NReduce
	m.NumWorkers.Lock()
	m.NumWorkers.Val++
	data.WorkerID = m.NumWorkers.Val
	m.NumWorkers.Unlock()
	return nil
}

func (m *Master) NewTaskID() int{
	m.TaskID.Lock()
	m.TaskID.Val++ 
	res  := m.TaskID.Val
	m.TaskID.Unlock()
	return res
}

func (s *SynchedFlag) Val() bool{
	s.Lock()
	res := s.Flag 
	s.Unlock()
	return res
}


func (s *SynchedFlag) Set(val bool){
	s.Lock()
	s.Flag = val 
	s.Unlock()
}

//functuons to help with synchronized data structures 

func (s *SynchedStack) Push(in []string) {
	s.Lock()
	s.Stack = append(s.Stack, in)
	s.Unlock()
}

func (s *SynchedStack) Pop() []string{
	s.Lock()
	if len(s.Stack) == 0{
		s.Unlock()
		return nil
	}
	res := s.Stack[len(s.Stack)-1]
	s.Stack = s.Stack[:len(s.Stack)-1]
	s.Unlock()
	return res
}

func (s *SynchedStack) Len() int{
	s.Lock()
	res := len(s.Stack)
	s.Unlock()
	return res
}

func (s *SynchedMap) Len() int{
	s.Lock()
	res := len(s.Map)
	s.Unlock()
	return res
}

func (s *SynchedMap) Add(key int ,value *TaskData){
	s.Lock()
	s.Map[key] = value
	s.Unlock()
}

//
func (m *Master) SendTask(args *ExampleArgs, task * Task) error {
	//if all maps haven't finished, waits until they do to proceed to reduce
	// if some map task fails redoing it's task 
	for {
		if m.MapDone.Val(){
			break
		}
		m.MapTasks.Lock()		
		tasksRunning := len(m.MapTasks.Stack)
		if tasksRunning != 0{
			m.MapTasks.Unlock()
			break
		}
		m.MapTasksRunning.Lock()
		tasksRunning += len(m.MapTasksRunning.Map)
		if tasksRunning == 0{
			m.MapDone.Set(true)
			m.MapTasksRunning.Unlock()
			m.MapTasks.Unlock()
			break
		}
		m.MapTasksRunning.Unlock()
		m.MapTasks.Unlock()
	 	time.Sleep(time.Second)
	 }
	//fmt.Println("sending task")
	task.TaskID = m.NewTaskID()
	tasksRunning := m.MapTasks.Len()
	if tasksRunning != 0 {
		task.InputFiles = m.MapTasks.Pop()
		task.TaskFn = "map"		
		m.MapTasksRunning.Add(task.TaskID, &TaskData{task.InputFiles, time.Now()})
	}else{
		tasksRunning = m.ReduceTasks.Len()
		tasksRunning += m.ReduceTasksRunning.Len()
		if tasksRunning != 0{
			task.InputFiles = m.ReduceTasks.Pop()
			task.TaskFn = "reduce"
			m.ReduceTasksRunning.Add(task.TaskID, &TaskData{task.InputFiles, time.Now()})
		}else{
			m.DoneFlag.Set(true) 
			return nil
		}
	}
	

	return nil
}

func RemoveFiles(files []string){
	for _,file := range files{
		err := os.Remove(file)
		if err != nil {
			fmt.Println(err)
			return
		}
	} 
}


// checkks if reported task has failed or not, if so it deletes returned files 
// otherwise adds map(if current task is map) task to reduce list and removes it from running tasks map
func (m *Master) GetReport(report *Report, args *ExampleReply) error {
	if report.TaskFn == "map"{
		m.MapTasksRunning.Lock()
		_, ok := m.MapTasksRunning.Map[report.TaskID]
		if !ok{
			RemoveFiles(report.OutputFiles)
		}else{
			m.ReduceTasks.Lock()
			//adds output files to corresponding reduce index
			for i,file := range report.OutputFiles {
				m.ReduceTasks.Stack[i] = append(m.ReduceTasks.Stack[i], file) 
			} 
			m.ReduceTasks.Unlock()
			delete(m.MapTasksRunning.Map, report.TaskID)
		}
		m.MapTasksRunning.Unlock()
	}else{
		m.ReduceTasksRunning.Lock()
		_, ok := m.ReduceTasksRunning.Map[report.TaskID]
		if !ok{
			RemoveFiles(report.OutputFiles)
		}else{
			delete(m.ReduceTasksRunning.Map, report.TaskID)
		}
		m.ReduceTasksRunning.Unlock()
	}
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	//edge case when maps finished and reduces haven't started
	if m.DoneFlag.Val() && m.MapTasksRunning.Len() == 0 &&
		 m.ReduceTasksRunning.Len() == 0{
			return true
	}
	return false
}


// if  task started more than 10 seconds ago, removes it from running list 
// and adds its files to future tasks
func (m *Master) RecoverTasks(){
	for {
		if m.DoneFlag.Val() {
			return 
		}
		m.MapTasksRunning.Lock()
		for k,task := range m.MapTasksRunning.Map{
			if time.Now().Sub(task.Start).Seconds() >= 10{
				m.MapTasks.Push(task.TaskFiles)
				delete(m.MapTasksRunning.Map, k)
			}
		}
		m.MapTasksRunning.Unlock()
		m.ReduceTasksRunning.Lock()
		for k,task := range m.ReduceTasksRunning.Map{
			if time.Now().Sub(task.Start).Seconds() >= 10{
				m.ReduceTasks.Push(task.TaskFiles)
				delete(m.ReduceTasksRunning.Map, k)
			}
		}
		m.ReduceTasksRunning.Unlock()
		time.Sleep(1 * time.Second)
	}
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	//fmt.Println("master: started")
	
	m := Master{}
	m.NReduce = nReduce
	m.MapTasks.Stack = make([][]string, 0)
	m.ReduceTasks.Stack = make([][]string, 15)
	m.MapTasksRunning.Map = make(map[int] *TaskData)
	m.ReduceTasksRunning.Map = make(map[int] *TaskData)

	for _,file := range files{
		m.MapTasks.Push([]string{file})
	}

	go m.RecoverTasks()
	
	m.server()
	return &m
}
