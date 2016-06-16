// 2015 Jamie Alquiza
package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type Statser interface {
	IncrSent(int64)
	FetchSent() int64
}

type MessageBatch struct {
	body []byte
	count int
	limitSize int
}

var esResponse struct {
	Took int `json:"took"`
	Errors bool `json:"errors"`
	Items []interface{} `json:"items"`
}

var (
	outgoingRequestQueue = make(chan bytes.Buffer)
	bulkRequest bytes.Buffer
)

// Run accepts a message queue and several threshold values (time, size and count).
// Messages are read and accumulated into a batch. The batch will be enqueued for
// writing to ElasticSearch when the any of the thresholds are met.
func Run(es string, messageIncomingQueue <-chan []byte, writers int, timeout int, size int) {
	for i := 0; i < writers; i++ {
		go bulkWriter(es)
	}

	flushTimeout := time.Tick(time.Duration(timeout) * time.Second)
	batch := &MessageBatch{limitSize: size}

	for {
		select {
		// If we hit the flush timeout, enqueue the current message batch.
		case <- flushTimeout:
			if len(batch.body) != 0 {
				log.Printf("Flushing %d document(s)", batch.count)
				enqueueRequest(batch)
			}
		case m:= <- messageIncomingQueue:
			// Deserialize message so we can check and mutate its contents.
			var parsed map[string]interface{}
			err := json.Unmarshal(m, &parsed)
			if err != nil {
				// Isn't json, do this
				log.Printf("Invalid json: %s\n", err)
				return
			}

			// Check if the @timestamp field was provided.
			// If not, we append the time of message handling.
			if _, ok := parsed["@timestamp"]; !ok {
				parsed["@timestamp"] = time.Now().Format(time.RFC3339)
			}

			// Check if @type is defined.
			// This will dictate the ElasticSearch document type.
			docType, ok := parsed["@type"]; if !ok {
				docType = "json"
			}

			// Serialize the message and append to the current message batch.
			parsedEncoded, _ := json.Marshal(&parsed)

			// If the count or size threshold is met, enqueue the batch for writing.
			// Otherwise, just keep appending messages.
			threshold := batch.AppendDocument(parsedEncoded, docType.(string))
			if threshold != "" {
				log.Println(threshold)
				enqueueRequest(batch)
			}
		}
	}
}

// enqueueRequest reads the current batch into a buffer,
// enques into the outgoingRequestQueue and reset objects.
func enqueueRequest(b *MessageBatch) {
	// Read batch into buffer, enque and reset objects.
	bulkRequest.Write(b.body)
	outgoingRequestQueue <- bulkRequest
	bulkRequest.Reset()
	b.body = []byte{}
	b.count = 0
}

// bulkWriter pops request objects from the outgoingRequestQueue
// and writes to ElasticSearch.
func bulkWriter(es string) {
	client := &http.Client{}
	for r := range outgoingRequestQueue {
		// Send bulk req to ES.
		resp, err := client.Post(es+"/_bulk", "application/json", &r)
		if err != nil {
			log.Println(err)
			return
		}
		defer resp.Body.Close()

		// Handle response from ES.
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return
		}

		var respJson = esResponse
		json.Unmarshal(body, &respJson)
		if respJson.Errors {
			// Should pause new indexing requests and retry this batch.
			verbose, _ := json.Marshal(respJson)
			log.Println(verbose)
		} else {
			log.Printf("Indexed %d document(s) in %dms", len(respJson.Items), respJson.Took)
		}
	}
}

// AppendDocument generates an ElasticSearch bulk request
// header and appends it along with a message to the current
// batch. It also checks if the batch size is over the configured 
// threshold upon each append and will return a threshold
// trigger to the caller.
func (b *MessageBatch) AppendDocument(m []byte, t string) string {
	headerTemplate := map[string]map[string]string{"index": {}}
	headerTemplate["index"]["_index"] = "recon"
	headerTemplate["index"]["_type"] = t
	header, _ := json.Marshal(&headerTemplate)

	b.body = append(b.body, header...)
	b.body = append(b.body, 10)
	b.body = append(b.body, m...)
	b.body = append(b.body, 10)
	b.count++

	// We check the whole batch size including the bulk
	// request actions meta. May want to reduce this to purely metering
	// the request data size.
	if len(b.body) >= b.limitSize {
		count := b.count
		b.count = 0
		return fmt.Sprintf("Batch at size threshold, flushing %d messages", count)
	}
	return ""
}
