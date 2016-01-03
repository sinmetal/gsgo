package gsgo

import (
	"bufio"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

const Kind = "Sample2"

type Sample struct {
	Values []float64 `datastore:",noindex"`
}

func init() {
	http.HandleFunc("/", handler)
	http.HandleFunc("/read", handlerFileRead)
	http.HandleFunc("/queue/data", handlerDataPut)
}

func handler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello File Reader"))
}

func handlerFileRead(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	log.Infof(c, "Hello Blobstore")

	file := r.FormValue("file")
	log.Infof(c, "File Param = %s", file)

	bKey, err := blobstore.BlobKeyForFile(c, "/gs/workbucket/"+file)
	if err != nil {
		log.Infof(c, "Blobstore:BlobKeyForFileError, err = %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reader := blobstore.NewReader(c, bKey)

	cnt := 0
	var tasks = make([]*taskqueue.Task, 0, 30)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		//log.Infof(c, "%s", scanner.Text())
		cnt++
		tasks = append(tasks, &taskqueue.Task{
			Path:    "/queue/data",
			Payload: []byte(scanner.Text()),
			Method:  "POST",
		})
		if cnt%30 == 0 {
			log.Infof(c, "read count = %d", cnt)
			_, err = taskqueue.AddMulti(c, tasks, "dataloader")
			if err != nil {
				log.Errorf(c, "add taskqueue : %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			tasks = make([]*taskqueue.Task, 0, 30)
		}
	}
	log.Infof(c, "read count = %d", cnt)
	_, err = taskqueue.AddMulti(c, tasks, "dataloader")
	if err != nil {
		log.Errorf(c, "add taskqueue : %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := scanner.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("done."))
}

func handlerDataPut(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if r.Method != "POST" {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	nc, err := appengine.Namespace(c, "testfilereader")
	if err != nil {
		log.Errorf(c, "create namespace: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf(c, "body read error : %v", err)
	}

	body := string(b)
	log.Infof(c, "request body = %s", body)
	arr := strings.Split(body, ",")
	l := len(arr)
	log.Infof(c, "requry body array len = %d", l)

	key := datastore.NewKey(nc, Kind, arr[0], 0, nil)
	q := datastore.NewQuery(Kind).Filter("__key__ =", key)
	q = q.KeysOnly()
	var samples []Sample
	keys, err := q.GetAll(nc, &samples)
	if err != nil {
		log.Errorf(c, "fetching sample: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(keys) > 0 {
		log.Infof(c, "path.")
		w.WriteHeader(http.StatusOK)
		return
	}

	values := make([]float64, len(arr)-1, len(arr)-1)
	for i := 1; i < len(arr); i++ {
		values[i-1], err = strconv.ParseFloat(arr[i], 64)
		if err != nil {
			log.Errorf(c, "parse float: %v, value = %s", err, arr[i])
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	s := &Sample{
		Values: values,
	}
	_, err = datastore.Put(nc, key, s)
	if err != nil {
		log.Errorf(c, "put sample: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof(c, "done.")
	w.WriteHeader(http.StatusCreated)
}
