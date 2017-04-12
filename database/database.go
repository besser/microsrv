package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	ID    int `json:"id"`
	State int `json:"state"`
}

var (
	datastore             []Task
	datastoreMutex        sync.RWMutex
	oldestNotFinishedTask int // remember to account for potential int overflow in production. Use something bigger.
	oNFTMutex             sync.RWMutex
)

func main() {
	if !registerInKVStore() {
		return
	}

	datastore = make([]Task, 0)
	datastoreMutex = sync.RWMutex{}
	oldestNotFinishedTask = 0
	oNFTMutex = sync.RWMutex{}

	http.HandleFunc("/getByID", getByID)
	http.HandleFunc("/newTask", newTask)
	http.HandleFunc("/getNewTask", getNewTask)
	http.HandleFunc("/finishTask", finishTask)
	http.HandleFunc("/setByID", setByID)
	http.HandleFunc("/list", list)
	http.ListenAndServe(":3001", nil)
}

func getByID(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		id, err := strconv.Atoi(string(values.Get("id")))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		datastoreMutex.RLock()
		bIsInError := err != nil || id >= len(datastore) // Reading the length of a slice must be done in a synchronized manner. That's why the mutex is used.
		datastoreMutex.RUnlock()

		if bIsInError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		datastoreMutex.RLock()
		value := datastore[id]
		datastoreMutex.RUnlock()

		response, err := json.Marshal(value)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(response))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}
}

func getNewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		bErrored := false

		datastoreMutex.RLock()
		if len(datastore) == 0 {
			bErrored = true
		}
		datastoreMutex.RUnlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: No non-started task.")
			return
		}

		taskToSend := Task{ID: -1, State: 0}

		oNFTMutex.Lock()
		datastoreMutex.Lock()
		for i := oldestNotFinishedTask; i < len(datastore); i++ {
			if datastore[i].State == 2 && i == oldestNotFinishedTask {
				oldestNotFinishedTask++
				continue
			}
			if datastore[i].State == 0 {
				datastore[i] = Task{ID: i, State: 1}
				taskToSend = datastore[i]
				break
			}
		}
		datastoreMutex.Unlock()
		oNFTMutex.Unlock()

		if taskToSend.ID == -1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: No non-started task.")
			return
		}

		myID := taskToSend.ID

		go func() {
			time.Sleep(time.Second * 120)
			datastoreMutex.Lock()
			if datastore[myID].State == 1 {
				datastore[myID] = Task{ID: myID, State: 0}
			}
			datastoreMutex.Unlock()
		}()

		response, err := json.Marshal(taskToSend)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(response))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func finishTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		id, err := strconv.Atoi(string(values.Get("id")))

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		updatedTask := Task{ID: id, State: 2}

		bErrored := false

		datastoreMutex.Lock()
		if datastore[id].State == 1 {
			datastore[id] = updatedTask
		} else {
			bErrored = true
		}
		datastoreMutex.Unlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input")
			return
		}

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		datastoreMutex.RLock()
		for key, value := range datastore {
			fmt.Fprintln(w, key, ": ", "id:", value.ID, " state:", value.State)
		}
		datastoreMutex.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}

}

func newTask(w http.ResponseWriter, r *http.Request) {

	if r.Method == http.MethodPost {
		datastoreMutex.Lock()
		taskToAdd := Task{
			ID:    len(datastore),
			State: 0,
		}
		datastore[taskToAdd.ID] = taskToAdd
		datastoreMutex.Unlock()

		fmt.Fprint(w, taskToAdd.ID)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func setByID(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		taskToSet := Task{}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}
		err = json.Unmarshal([]byte(data), &taskToSet)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		bErrored := false
		datastoreMutex.Lock()
		if taskToSet.ID >= len(datastore) || taskToSet.State > 2 || taskToSet.State < 0 {
			bErrored = true
		} else {
			datastore[taskToSet.ID] = taskToSet
		}
		datastoreMutex.Unlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input")
			return
		}

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error: Too few arguments.")
		return false
	}

	databaseAddress := os.Args[1] // The address of itself
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=databaseAddress&value="+databaseAddress, "", nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: Failure when contacting key-value store: ", string(data))
		return false
	}

	return true
}
