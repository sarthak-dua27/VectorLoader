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

func uploadToCouchbase(id int, wg *sync.WaitGroup, collection *gocb.Collection, dataset string) {
	defer wg.Done()
	doc := getDocuments(1, dataset)
	var err error
	for retry := 0; retry < 5; retry++ {
		var err error
		if dataset == "car" {
			_, err = collection.Upsert(strconv.Itoa(id), (*doc.CarDocument)[0], nil)
		} else {
			_, err = collection.Upsert(strconv.Itoa(id), (*doc.StoreDocument)[0], nil)
		}
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
	var collectionName1 string
	var collectionName2 string
	var username string
	var password string
	var startIndex int
	var endIndex int
	var batchSize int

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&scopeName, "scopeName", "_default", "Scope name")
	flag.StringVar(&collectionName1, "collectionName1", "_default", "Collection name 1")
	flag.StringVar(&collectionName2, "collectionName2", "store", "Collection name 2")
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

	col := bucket.Scope(scopeName).Collection(collectionName1)

	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			go uploadToCouchbase(j, &wg, col, "car")
		}
		wg.Wait()
		startIndex = end
	}
	startIndex = 0
	endIndex = 15
	col = bucket.Scope(scopeName).Collection(collectionName2)

	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			go uploadToCouchbase(j, &wg, col, "store")
		}
		wg.Wait()
		startIndex = end
	}
}
