package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "os"
import "io/ioutil"

import "encoding/json"

import "sort"
import "time"
import "strconv"



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerData struct{
	WorkerID int 
	NReduce int
	MapFn func(string, string) []KeyValue
	ReduceFn func(string, []string) string
}


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapTask(input string, taskID int, worker WorkerData) []string{

	file, err := os.Open(input)
	if err != nil {
		log.Fatalf("cannot open %v", input)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", input)
	}
	file.Close()
	kva := worker.MapFn(input, string(content))

	encs := make(map[int]*json.Encoder)	
	files := []string{}
	
	for i:=0; i<worker.NReduce; i++ {
		tempFile, _ := ioutil.TempFile(".","")
		files = append(files, tempFile.Name()) 
		encs[i] = json.NewEncoder(tempFile)
	}

	for _,kv := range kva {
		index := ihash(kv.Key) % worker.NReduce
		err := encs[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot enc ")
		}
	}

	for i:=0; i< worker.NReduce ;i++{
		oname := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(i) + ".tmp"
		os.Rename(files[i],oname)
		files[i] = oname
	}
	return files
}

func ReduceTask(inputs []string,taskID int , worker WorkerData) []string{
	kva := []KeyValue{}
	for _,input :=range inputs{
		file, err := os.Open(input)
		if err != nil {
			log.Fatalf("cannot open %v", input)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))	
	oname := "mr-out-"+ strconv.Itoa(taskID)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := worker.ReduceFn(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	return []string{oname}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	md := GetMetadata()
	worker := WorkerData{md.WorkerID, md.NReduce, mapf, reducef}
	
	for {
		//unite send/get in one rpc ??? or maybe not ?
		task := GetTask()
		if task.TaskFn == "map" {
			SendReport(MapTask(task.InputFiles[0], task.TaskID , worker), "map", task.TaskID)
		}else if task.TaskFn == "reduce" {
			SendReport(ReduceTask(task.InputFiles, task.TaskID, worker), "reduce", task.TaskID)
		}else{
			break
		}		
		time.Sleep(time.Second)
		// uncomment to send the Example RPC to the master.
		// CallExample()	
	}
	
}


func GetMetadata() Metadata{	
	metadata := Metadata{}
	call("Master.SendMetadata", &ExampleArgs{}, &metadata)
	return metadata
}

func GetTask() Task{
	task := Task{}
	call("Master.SendTask", &ExampleArgs{}, &task)
	return task
}


func SendReport(outputFiles []string, taskfn string, taskID int) {

	report := Report{taskID,taskfn, outputFiles}
	call("Master.GetReport", &report, &ExampleReply{})
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
