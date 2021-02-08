// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"bytes"
	"context"
	"encoding/base32"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

const (
	indexMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "import_id": {
        "type": "keyword"
      },
      "metric_type": {
        "type": "keyword"
      },
      "node": {
        "type": "keyword"
      },
      "pipeline": {
        "type": "keyword"
      },
      "processor_index": {
        "type": "long"
      },
      "processor_type": {
        "type": "keyword"
      },
      "processor_name": {
        "type": "keyword"
      },
      "count": {
        "type": "long"
      },
      "current": {
        "type": "long"
      },
      "failed": {
        "type": "long"
      },
      "time_in_millis": {
        "type": "long"
      },
      "pipeline_stats": {
        "properties": {
          "count": {
            "type": "long"
          },
          "current": {
            "type": "long"
          },
          "failed": {
            "type": "long"
          },
          "time_in_millis": {
            "type": "long"
          }
        }
      },
      "calculated": {
        "properties": {
          "execution_time_avg_ns": {
            "type": "long"
          },
          "time_pct": {
            "type": "float"
          },
          "failed_pct": {
            "type": "float"
          },
          "events_per_sec": {
            "type": "float"
          }
        }
      }
    }
  }
}
`
)

type NodeStats struct {
	Nodes map[string]Node `json:"nodes"`
}

type Ingest struct {
	Pipelines map[string]Pipeline `json:"pipelines"`
	Total     Stats               `json:"total"`
}

type ProcessorStats struct {
	Stats Stats  `json:"stats"`
	Type  string `json:"type"`
}

type Stats struct {
	Count        int64 `json:"count"`
	Current      int64 `json:"current"`
	Failed       int64 `json:"failed"`
	TimeInMillis int64 `json:"time_in_millis"`
}

type Node struct {
	Ingest Ingest `json:"ingest"`
}

type Pipeline struct {
	Stats
	Processors []map[string]ProcessorStats `json:"processors"`
}

type CommonFields struct {
	ImportID   string `json:"import_id"`
	MetricType string `json:"metric_type"`
	Node       string `json:"node"`
	Pipeline   string `json:"pipeline"`
}

type ProcessorStat struct {
	CommonFields
	Stats
	Index       int    `json:"processor_index"`
	Type        string `json:"processor_type"`
	Name        string `json:"processor_name"`
	ParentStats Stats  `json:"pipeline_stats"`
	Calculated  struct {
		TimeAverageNs float32 `json:"execution_time_avg_ns"` // Average execution time (ns).
		TimePercent   float32 `json:"time_pct"`              // Percentage of overall pipeline time.
		FailedPercent float32 `json:"failed_pct"`            // What percentage of executions failed.
	} `json:"calculated"`
}

func (s ProcessorStat) ID() string {
	h := sha3.New224()
	fmt.Fprint(h, s.ImportID)
	fmt.Fprint(h, s.MetricType)
	fmt.Fprint(h, s.Node)
	fmt.Fprint(h, s.Pipeline)
	fmt.Fprint(h, s.Index)
	return base32.StdEncoding.EncodeToString(h.Sum(nil))
}

type PipelineStat struct {
	CommonFields
	Stats
	Calculated struct {
		EventsPerSecond float64 `json:"events_per_sec"`
	} `json:"calculated"`
}

func (s PipelineStat) ID() string {
	h := sha3.New224()
	fmt.Fprint(h, s.ImportID)
	fmt.Fprint(h, s.MetricType)
	fmt.Fprint(h, s.Node)
	fmt.Fprint(h, s.Pipeline)
	return base32.StdEncoding.EncodeToString(h.Sum(nil))
}

type IDer interface {
	ID() string
}

var (
	index            string // Index to write to.
	importID         string // Unique ID added to events to distinguish one run from another.
	elasticsearchURL string
	username         string
	password         string
	apiKey           string
)

func init() {
	flag.StringVar(&index, "index", "pipeline-stats", "name of index to create")
	flag.StringVar(&importID, "import-id", strconv.FormatInt(time.Now().UnixNano(), 10), "import ID")
	flag.StringVar(&elasticsearchURL, "es-url", "http://localhost:9200", "Elasticsearch URL")
	flag.StringVar(&username, "u", "", "Elasticsearch username")
	flag.StringVar(&password, "p", "", "Elasticsearch password")
	flag.StringVar(&apiKey, "api-key", "", "Elasticsearch API key")
	flag.Usage = usage
}

func usage() {
	fmt.Fprintln(os.Stdout, "Usage:")
	fmt.Fprintln(os.Stdout, "    pipeline-stats [flags] node-stats-json-file")
	flag.PrintDefaults()
}

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal("An input file argument is required.")
	}

	stats, err := readNodeStats(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	docs := denormalizeIngestStats(stats)

	if err := bulkWrite(docs); err != nil {
		log.Fatal(err)
	}
}

func readNodeStats(inputFile string) (*NodeStats, error) {
	data, err := ioutil.ReadFile(inputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read %v: %w", inputFile, err)
	}

	var out *NodeStats
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err = dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to read json from file: %w", err)
	}

	return out, nil
}

func denormalizeIngestStats(nodeStats *NodeStats) []interface{} {
	var out []interface{}

	for nodeName, nodeData := range nodeStats.Nodes {
		for pipelineName, pipelineData := range nodeData.Ingest.Pipelines {
			pipelineStat := PipelineStat{
				CommonFields: CommonFields{
					ImportID:   importID,
					Node:       nodeName,
					MetricType: "pipeline",
					Pipeline:   pipelineName,
				},
				Stats: pipelineData.Stats,
			}

			if pipelineStat.TimeInMillis > 0 {
				totalTime := time.Duration(pipelineStat.TimeInMillis) * time.Millisecond
				pipelineStat.Calculated.EventsPerSecond = float64(pipelineStat.Count) / totalTime.Seconds()
			}

			out = append(out, pipelineStat)

			commonFields := pipelineStat.CommonFields
			commonFields.MetricType = "processor"

			for i, s := range pipelineData.Processors {
				for _, p := range s {
					ds := ProcessorStat{
						CommonFields: commonFields,
						Stats:        p.Stats,
						ParentStats:  pipelineData.Stats,
						Index:        i,
						Type:         p.Type,
						Name:         strconv.Itoa(i) + "_" + p.Type,
					}
					if ds.Stats.Count != 0 {
						ds.Calculated.TimeAverageNs = float32(ds.Stats.TimeInMillis) / float32(ds.Stats.Count) * 1e6
					}
					if ds.ParentStats.TimeInMillis != 0 {
						ds.Calculated.TimePercent = float32(ds.Stats.TimeInMillis) / float32(ds.ParentStats.TimeInMillis)
					}
					if ds.Stats.Count != 0 {
						ds.Calculated.FailedPercent = float32(ds.Stats.Failed) / float32(ds.Stats.Count)
					}
					out = append(out, ds)

					// Only need to get the first key which is the processor type (e.g. "append").
					break
				}
			}
		}
	}

	return out
}

func bulkWrite(docs []interface{}) error {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticsearchURL},
		APIKey:    apiKey,
		Username:  username,
		Password:  password,

		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},
		// Retry up to 5 attempts
		//
		MaxRetries: 5,
	})
	if err != nil {
		return fmt.Errorf("failed to create ES client: %w", err)
	}

	// Create index with mapping.
	res, err := es.Indices.Exists([]string{index})
	if err != nil {
		return err
	}
	switch res.StatusCode {
	case http.StatusOK:
		log.Printf("Index %q exists. Not creating.", index)
	case http.StatusNotFound:
		log.Printf("Creating new %q index.", index)
		res, err = es.Indices.Create(index, es.Indices.Create.WithBody(strings.NewReader(indexMapping)))
		if err != nil {
			return err
		}
		if res.IsError() {
			return fmt.Errorf("error creating index: %v", res)
		}
	default:
		return fmt.Errorf("error checking if index exists: %v", res)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         index,
		Client:        es,               // The Elasticsearch client
		NumWorkers:    1,                // The number of worker goroutines
		FlushBytes:    int(5e6),         // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil {
		fmt.Errorf("error creating the ES indexer: %w", err)
	}

	// Index docs.
	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshall doc [%#v] to JSON: %w", doc, err)
		}

		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: doc.(IDer).ID(),
				Body:       bytes.NewReader(data),
			},
		)
		if err != nil {
			return fmt.Errorf("failed indexing docs: %w", err)
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		return fmt.Errorf("failed to close bulk indexer: %w", err)
	}

	biStats := bi.Stats()
	if biStats.NumFailed > 0 {
		return fmt.Errorf("indexed %d documents, but there were %d errors", int64(biStats.NumFlushed), int64(biStats.NumFailed))
	}

	log.Printf("Sucessfuly indexed [%d] documents.", int64(biStats.NumFlushed))
	return nil
}
