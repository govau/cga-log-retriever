package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	bolt "go.etcd.io/bbolt"
)

const (
	dateFormat = "2006-01-02T15:04"
	keyFormat  = "2006/01/02/15/04/"
)

var (
	errNotThere = errors.New("not there")
)

type getRecordsJob struct {
	S3      *s3.S3
	DB      *bolt.DB
	Workers int

	jobs   chan *jobToDo
	output chan string
}

type jobToDo struct {
	Bucket    string
	KeyPrefix string
	Match     string
}

func (j *getRecordsJob) getKeys(bucket, prefix string) ([]string, error) {
	cacheKey := []byte(bucket + "|" + prefix)

	var keysInThing []string
	err := j.DB.View(func(tx *bolt.Tx) error {
		vs := tx.Bucket([]byte("keys")).Get(cacheKey)
		if len(vs) == 0 {
			return errNotThere
		}
		return gob.NewDecoder(bytes.NewReader(vs)).Decode(&keysInThing)
	})
	if err == nil {
		return keysInThing, nil
	}
	if err != errNotThere {
		return nil, err
	}

	// we need to lookup
	loo, err := j.S3.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	for _, item := range loo.Contents {
		keysInThing = append(keysInThing, *item.Key)
	}

	valToPut := &bytes.Buffer{}
	err = gob.NewEncoder(valToPut).Encode(keysInThing)
	if err != nil {
		return nil, err
	}

	err = j.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("keys")).Put(cacheKey, valToPut.Bytes())
	})
	if err != nil {
		return nil, err
	}

	return keysInThing, nil
}

func (j *getRecordsJob) getFile(bucket, key, match string) ([]string, error) {
	cacheKey := []byte(bucket + "|" + key + "|" + match)

	var linesInThing []string
	err := j.DB.View(func(tx *bolt.Tx) error {
		vs := tx.Bucket([]byte("logs")).Get(cacheKey)
		if len(vs) == 0 {
			return errNotThere
		}
		return gob.NewDecoder(bytes.NewReader(vs)).Decode(&linesInThing)
	})
	if err == nil {
		return linesInThing, nil
	}
	if err != errNotThere {
		return nil, err
	}

	// we need to lookup
	goo, err := j.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer goo.Body.Close()

	// AWS library seems to un-gzip for us...
	/*gzr, err := gzip.NewReader(goo.Body)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()*/

	bits := strings.Split(match, ",")

	bui := bufio.NewReader(goo.Body)
	for {
		l, err := bui.ReadString('\n')

		didMatch := true
		for _, bit := range bits {
			if strings.Index(l, bit) == -1 {
				didMatch = false
				break
			}
		}
		if didMatch {
			linesInThing = append(linesInThing, l)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	valToPut := &bytes.Buffer{}
	err = gob.NewEncoder(valToPut).Encode(linesInThing)
	if err != nil {
		return nil, err
	}

	err = j.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("logs")).Put(cacheKey, valToPut.Bytes())
	})
	if err != nil {
		return nil, err
	}

	return linesInThing, nil
}

func (j *getRecordsJob) workerThread(wg *sync.WaitGroup) {
	defer wg.Done()

	counter := 0
	for job := range j.jobs {
		if counter%10 == 0 {
			log.Println("Progress: ", job.KeyPrefix)
		}
		counter++
		rs, err := j.getKeys(job.Bucket, job.KeyPrefix)
		if err != nil {
			log.Println("err", job.KeyPrefix, err)
		} else {
			for _, k := range rs {
				lines, err := j.getFile(job.Bucket, k, job.Match)
				if err != nil {
					log.Println("err", k, err)
				} else {
					for _, l := range lines {
						j.output <- l
					}
				}
			}
		}
	}
}

func (j *getRecordsJob) Find(bucket string, start, end time.Time, match string) error {
	j.jobs = make(chan *jobToDo, 1000)
	j.output = make(chan string, 1000)
	wgOutput := &sync.WaitGroup{}
	wgOutput.Add(1)

	go func() {
		defer wgOutput.Done()
		for line := range j.output {
			fmt.Println(strings.TrimSpace(line))
		}
	}()
	wg := &sync.WaitGroup{}
	wg.Add(j.Workers)
	for i := 0; i < j.Workers; i++ {
		go j.workerThread(wg)
	}
	for n := start; n.Before(end); n = n.Add(time.Minute) {
		j.jobs <- &jobToDo{
			Bucket:    bucket,
			KeyPrefix: n.Format(keyFormat),
			Match:     match,
		}
	}
	close(j.jobs)
	wg.Wait()
	close(j.output)
	wgOutput.Wait()
	return nil
}

func main() {
	var bucket, startDate, endDate, match string

	flag.StringVar(&bucket, "bucket", "", "bucket name containing logs")
	flag.StringVar(&startDate, "start", "", "start date in "+dateFormat)
	flag.StringVar(&endDate, "end", "", "end date in "+dateFormat)
	flag.StringVar(&match, "match", "", "string to look for")

	flag.Parse()

	if bucket == "" {
		log.Fatal("must specify bucket")
	}

	if match == "" {
		log.Fatal("must specify match")
	}

	start, err := time.Parse(dateFormat, startDate)
	if err != nil {
		log.Fatal(err)
	}

	end, err := time.Parse(dateFormat, endDate)
	if err != nil {
		log.Fatal(err)
	}

	sess, err := session.NewSession()
	if err != nil {
		log.Fatal(err)
	}

	logsLocation := os.Getenv("LOGS_DB")
	if logsLocation == "" {
		logsLocation = ".logsdb"
	}

	db, err := bolt.Open(logsLocation, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range []string{"keys", "logs"} {
			_, err := tx.CreateBucketIfNotExists([]byte(b))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close() // since log.Fatal will eat our defer
		log.Fatal(err)
	}

	err = (&getRecordsJob{
		DB:      db,
		S3:      s3.New(sess),
		Workers: 20,
	}).Find(bucket, start, end, match)
	if err != nil {
		db.Close() // since log.Fatal will eat our defer
		log.Fatal(err)
	}
}
