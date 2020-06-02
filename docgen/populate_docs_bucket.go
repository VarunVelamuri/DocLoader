package docgen

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

var Username, Password, Hostaddress string

func Init(username, password, hostaddress string) {
	Username = username
	Password = password
	Hostaddress = hostaddress
}

var totalThreads = runtime.NumCPU()

/*
toBucket => true means that all the documents will be
pushed to bucket but the type of documents will depend
on the scope and collections
*/
func PopulateDocs(bucket string, numDocs, totalStack int64) {
	if totalStack > 1000 {
		return
	}
	diff := populateDocs(bucket, numDocs)
	if diff != nil {
		for _, value := range diff[Hostaddress] {
			totalStack++
			PopulateDocs(bucket, value, totalStack)
		}
	} else {
		return
	}
}
func populateDocs(bucket string, numDocs int64) map[string]map[string]int64 {
	maxDocsPerIter := (int64)(5000000)

	numDocsPending := numDocs
	for {
		if numDocsPending > maxDocsPerIter {
			numDocsPending = numDocsPending - maxDocsPerIter
			pushDocs(bucket, maxDocsPerIter)
		} else {
			pushDocs(bucket, numDocsPending)
			break
		}
	}
	time.Sleep(5 * time.Second)
	return nil
}
func pushDocs(bucket string, numDocs int64) {

	// a. Generate docs for populating to bucket
	var wg1 sync.WaitGroup
	before := time.Now().UnixNano()
	docCh := make([]chan map[string]interface{}, totalThreads)
	stepPerThread := make([]int64, totalThreads)

	// b. Generate slice of steps. last thread will push remaining docs
	step := numDocs / (int64)(totalThreads)
	sum := (int64)(0)
	for tid := 0; tid < totalThreads; tid++ {
		sum += step
		if tid == totalThreads-1 {
			stepPerThread[tid] = step + (numDocs - sum)
		} else {
			stepPerThread[tid] = step
		}
	}
	log.Printf("Steps per thread: %v", stepPerThread)

	for tid := 0; tid < totalThreads; tid++ {
		docCh[tid] = make(chan map[string]interface{}, stepPerThread[tid])
		wg1.Add(1)
		go func(index int) {
			defer wg1.Done()
			seed := rand.New(rand.NewSource(time.Now().UnixNano()))
			var i int64
			for i = 0; i < stepPerThread[index]; i++ {
				doc := generateJson(String(100, seed), seed)
				docCh[index] <- doc
			}
			return
		}(tid)
	}
	wg1.Wait()
	after := time.Now().UnixNano()
	log.Printf("Time taken to generate docs for numDocs: %v is: %v", numDocs, after-before)

	var wg2 sync.WaitGroup
	for tid := 0; tid < totalThreads; tid++ {
		wg2.Add(1)
		go func(tid int) {
			defer wg2.Done()

			seed := rand.New(rand.NewSource(time.Now().UnixNano()))
			// Connect to a bukcet
			url := "http://" + bucket + ":" + Password + "@" + Hostaddress
			b, err := common.ConnectBucket(url, "default", bucket)
			if err != nil {
				panic(err)
			}
			defer b.Close()
			pushDoc := func(index int, doc map[string]interface{}, len int) {
				doc["type"] = bucket
				retry := 0
				for {
					key := "Users-" + String(20, seed) + fmt.Sprintf("-%v-%v", index, len)
					err = b.Set(key, 0, doc)
					if err != nil {
						log.Printf("[Error] Reveived error: %v", err)
						time.Sleep(30 * time.Second)
						retry++
						if retry > 5 {
							return
						}
					} else {
						return
					}
				}
			}
			// Push docs
			for {
				select {
				case doc := <-docCh[tid]:
					pushDoc(tid, doc, len(docCh[tid]))
				default:
					return
				}
			}
		}(tid)
	}
	wg2.Wait()
}
