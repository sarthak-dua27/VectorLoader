package main

import (
	"errors"
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
	var capella bool
	var numStores int

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "LetsGoShopping", "Bucket name")
	flag.StringVar(&scopeName, "scopeName", "CarComplex", "Scope name")
	flag.StringVar(&collectionName1, "collectionName1", "car", "Collection name 1")
	flag.StringVar(&collectionName2, "collectionName2", "store", "Collection name 2")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 100, "batchSize")
	flag.IntVar(&numStores, "num stores", 15, "number of stores")
	flag.BoolVar(&capella, "capella", false, "capella")

	flag.Parse()

	var cluster *gocb.Cluster
	var er error
	if capella {
		options := gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
			SecurityConfig: gocb.SecurityConfig{
				TLSSkipVerify: true,
			},
		}
		if err := options.ApplyProfile(gocb.
			ClusterConfigProfileWanDevelopment); err != nil {
			log.Fatal(err)
		}
		cluster, er = gocb.Connect(nodeAddress, options)
	} else {
		// Initialize the Connection
		cluster, er = gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
		})
	}

	if er != nil {
		panic(fmt.Errorf("error creating cluster object : %v", er))
	}

	createUtilities(cluster, bucketName, scopeName, []string{collectionName1, collectionName2})
	bucket := cluster.Bucket(bucketName)

	err := bucket.WaitUntilReady(20*time.Second, nil)
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
	endIndex = numStores
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

func createUtilities(cluster *gocb.Cluster, bucketName string, scopeName string, collectionName []string) {
	bucketMgr := cluster.Buckets()
	err := bucketMgr.CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:                 bucketName,
			FlushEnabled:         true,
			ReplicaIndexDisabled: true,
			RAMQuotaMB:           1024,
			NumReplicas:          0,
			BucketType:           gocb.CouchbaseBucketType,
		},
		ConflictResolutionType: gocb.ConflictResolutionTypeSequenceNumber,
	}, nil)
	if err != nil {
		fmt.Println("Error creating bucket:", err)
	} else {
		fmt.Println("Bucket created successfully.")
	}

	bucket := cluster.Bucket(bucketName)

	collections := bucket.Collections()

	err = collections.CreateScope(scopeName, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrScopeExists) {
			fmt.Println("Scope already exists")
		} else {
			panic(err)
		}
	}

	for _, collection := range collectionName {
		collection := gocb.CollectionSpec{
			Name:      collection,
			ScopeName: scopeName,
		}

		err = collections.CreateCollection(collection, nil)
		if err != nil {
			if errors.Is(err, gocb.ErrCollectionExists) {
				fmt.Println("Collection already exists")
			} else {
				panic(err)
			}
		}
	}
}
