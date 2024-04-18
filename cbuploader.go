package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

func uploadToCouchbase(id int, wg *sync.WaitGroup, collection *gocb.Collection, dataset string) {
	defer wg.Done()
	doc, e := getDocuments(1, dataset)
	if e != nil {
		log.Printf("Error generating document %v", e)
		return
	}
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

func uploadColorDocuments(index int, wg *sync.WaitGroup, collection *gocb.Collection, document Color) {
	defer wg.Done()
	var err error
	for retry := 0; retry < 5; retry++ {
		var err error
		_, err = collection.Upsert(strconv.Itoa(index), document, nil)
		if err != nil {
			fmt.Printf("error %v\n", err)
			time.Sleep(2 * time.Second)
		} else {
			//fmt.Printf("Done uploading %v docuemnt\n", index)
			err = nil
			break
		}
	}
	if err != nil {
		log.Printf("Error inserting document %s: %v", strconv.Itoa(index), err)
	}

}

func main() {
	var nodeAddress string
	var bucketName string
	var scopeName string
	var collectionName1 string
	var collectionName2 string
	var collectionName3 string
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
	flag.StringVar(&collectionName3, "collectionName3", "colors", "Collection name 3")
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

	createUtilities(cluster, bucketName, scopeName, []string{collectionName2})
	bucket := cluster.Bucket(bucketName)

	err := bucket.WaitUntilReady(20*time.Second, nil)
	if err != nil {
		panic(err)
	}

	col := bucket.Scope(scopeName).Collection(collectionName1)

	// upload car data
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

	// upload store data
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

	// upload colors data
	data, e := ioutil.ReadFile("./colors.json")
	if e != nil {
		fmt.Printf("Error reading colors file %v\n", e)
		return
	}
	var colorsobj Colors
	e = json.Unmarshal(data, &colorsobj)
	if e != nil {
		fmt.Printf("Error reading colors file %v\n", e)
		return
	}

	for i := range colorsobj.Colors {

		color := &colorsobj.Colors[i]
		vec, e := get_rgb_from_hex(color.Hex)
		color.Vector = vec
		if e != nil {
			fmt.Printf("Error getting rgb from hex%v\n", e)
			return
		}
	}

	startIndex = 0
	endIndex = len(colorsobj.Colors)
	col = bucket.Scope(scopeName).Collection(collectionName3)
	if batchSize > endIndex {
		batchSize = endIndex / 3
	}
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			//fmt.Println(j)
			go uploadColorDocuments(j, &wg, col, colorsobj.Colors[j])
		}
		wg.Wait()
		startIndex = end
	}

}

func createUtilities(cluster *gocb.Cluster, bucketName string, scopeName string, collectionName []string) {
	//bucketMgr := cluster.Buckets()
	//err := bucketMgr.CreateBucket(gocb.CreateBucketSettings{
	//	BucketSettings: gocb.BucketSettings{
	//		Name:                 bucketName,
	//		FlushEnabled:         true,
	//		ReplicaIndexDisabled: true,
	//		RAMQuotaMB:           1024,
	//		NumReplicas:          0,
	//		BucketType:           gocb.CouchbaseBucketType,
	//	},
	//	ConflictResolutionType: gocb.ConflictResolutionTypeSequenceNumber,
	//}, nil)
	//if err != nil {
	//	fmt.Println("Error creating bucket:", err)
	//} else {
	//	fmt.Println("Bucket created successfully.")
	//}

	bucket := cluster.Bucket(bucketName)

	collections := bucket.Collections()

	err := collections.CreateScope(scopeName, nil)
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

		err := collections.CreateCollection(collection, nil)
		if err != nil {
			if errors.Is(err, gocb.ErrCollectionExists) {
				fmt.Printf("collection %s already exists\n", collection.Name)
			} else {
				panic(err)
			}
		}
	}
}
