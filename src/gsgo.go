package gsgo

import (
	"bufio"
	"net/http"

	"google.golang.org/appengine"
	"google.golang.org/appengine/blobstore"
	"google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/", handler)
	http.HandleFunc("/read", handlerFileRead)
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
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		//log.Infof(c, "%s", scanner.Text())
		cnt++
		if cnt%1000 == 0 {
			log.Infof(c, "read count = %s", cnt)
		}
	}
	log.Infof(c, "read count = %s", cnt)
	if err := scanner.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("done."))
}
