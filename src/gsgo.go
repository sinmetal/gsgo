package gsgo

import (
	"bufio"
	"encoding/csv"
	"io"
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

	brd := blobstore.NewReader(c, bKey)

	cnt := 0
	//var tasks = make([]*taskqueue.Task, 0, 30)
	reader := bufio.NewReaderSize(brd, 1024*1024*10)
	for {
		line, isPrefix, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Errorf(c, "read line : %v, isPrefix = %v", err, isPrefix)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if isPrefix {
			log.Errorf(c, "read line isPrefix = true")
			http.Error(w, "read line isPrefix = true", http.StatusBadRequest)
			return
		}
		log.Infof(c, "start : %s", line[0:100])
		log.Infof(c, "end : %s", line[len(line)-101:len(line)-1])

		cnt++
		t := &taskqueue.Task{
			Header: http.Header{
				"X-App-Version": {appengine.VersionID(c)},
			},
			Path:    "/queue/data",
			Payload: line,
			Method:  "POST",
		}
		_, err = taskqueue.Add(c, t, "dataloader")
		if err != nil {
			log.Errorf(c, "add taskqueue : %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	log.Infof(c, "read count = %d", cnt)

	w.Write([]byte("done."))
}

func handlerDataPut(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	for k, v := range r.Header {
		log.Infof(c, "%s:%s", k, v)
	}
	v := r.Header.Get("X-Appengine-Taskretrycount")
	log.Infof(c, "value = %v", v)
	if v != "0" {
		log.Infof(c, "stop retry!")
		w.WriteHeader(http.StatusOK)
		return
	}

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
		log.Errorf(c, "read request body : %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	body := string(b)
	log.Infof(c, "request body = %s", body)

	reader := csv.NewReader(strings.NewReader(body))
	record, err := reader.Read()
	if err != nil {
		log.Errorf(c, "encode csv : %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_, err = reader.Read()
	if err != io.EOF {
		log.Errorf(c, "multi line csv : %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	l := len(record)
	log.Infof(c, "requry body array len = %d", l)

	key := datastore.NewKey(nc, Kind, record[0], 0, nil)
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

	values := make([]float64, len(record)-1, len(record)-1)
	for i := 1; i < len(record); i++ {
		values[i-1], err = strconv.ParseFloat(record[i], 64)
		if err != nil {
			log.Errorf(c, "parse float: %v, value = %s", err, record[i])
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
