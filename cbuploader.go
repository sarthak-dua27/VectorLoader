package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

func uploadToCouchbase(id int, wg *sync.WaitGroup, collection *gocb.Collection) {
	defer wg.Done()
	doc := getDocuments(1)[0]
	var err error
	for retry := 0; retry < 5; retry++ {
		_, err = collection.Upsert(strconv.Itoa(id), doc, nil)
		if err != nil {
			time.Sleep(2 * time.Second)
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		log.Printf("Error inserting document %s: %v", strconv.Itoa(id), err)
	}
}

func main() {
	var nodeAddress string
	var bucketName string
	var scopeName string
	var collectionName string
	var username string
	var password string
	var startIndex int
	var endIndex int
	var batchSize int

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&scopeName, "scopeName", "_default", "Scope name")
	flag.StringVar(&collectionName, "collectionName", "_default", "Collection name")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 100, "batchSize")

	flag.Parse()

	// Initialize the Connection
	cluster, err := gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})

	if err != nil {
		panic(fmt.Errorf("error creating cluster object : %v", err))
	}
	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	col := bucket.Scope(scopeName).Collection(collectionName)

	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			go uploadToCouchbase(j, &wg, col)
		}
		wg.Wait()
		startIndex = end
	}
}
