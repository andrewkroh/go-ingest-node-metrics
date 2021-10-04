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
	_ "embed"
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

var (
	//go:embed assets/mapping.json
	indexMapping string
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
	Definition  string `json:"definition,omitempty"`
	Tag         string `json:"tag,omitempty"`
	Conditional *bool  `json:"conditional,omitempty"`
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

type IngestPipeline struct {
	Description string                       `json:"description"`
	Processors  []map[string]IngestProcessor `json:"processors"`
	OnFailure   []map[string]IngestProcessor `json:"on_failure"`
}

type IngestProcessor struct {
	ingestProcessor
	raw json.RawMessage
}

type ingestProcessor struct {
	If  *string `json:"if,omitempty"`
	Tag *string `json:"tag,omitempty"`
}

func (p *IngestProcessor) UnmarshalJSON(b []byte) error {
	var inner ingestProcessor
	if err := json.Unmarshal(b, &inner); err != nil {
		return err
	}

	p.ingestProcessor = inner
	p.raw = b
	return nil
}

var (
	pipelinesJSONFile string // Path to pipeline definitions JSON file (optional).
	index             string // Index to write to.
	importID          string // Unique ID added to events to distinguish one run from another.
	elasticsearchURL  string
	username          string
	password          string
	apiKey            string
)

func init() {
	flag.StringVar(&pipelinesJSONFile, "pipelines-json", "", "pipeline definitions from GET _ingest/pipeline")
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

	var pipelines map[string]*IngestPipeline
	if pipelinesJSONFile != "" {
		pipelines, err = readPipelines(pipelinesJSONFile)
		if err != nil {
			log.Fatal(err)
		}
	}

	docs := denormalizeIngestStats(stats, pipelines)

	if err := bulkWrite(docs); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Import ID:", importID)
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

func denormalizeIngestStats(nodeStats *NodeStats, pipelines map[string]*IngestPipeline) []interface{} {
	var out []interface{}

	for nodeName, nodeData := range nodeStats.Nodes {
		for pipelineName, pipelineData := range nodeData.Ingest.Pipelines {
			var pipelineDef *IngestPipeline
			if pipelines != nil {
				pipelineDef = pipelines[pipelineName]
			}

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
					// Incorporate data from the pipeline definition into the stats.
					processorType := p.Type
					var processorDef *IngestProcessor
					if pipelineDef != nil && len(pipelineDef.Processors) > i {
						for name, def := range pipelineDef.Processors[i] {
							processorType = name
							processorDef = &def
							break
						}
					}

					ds := ProcessorStat{
						CommonFields: commonFields,
						Stats:        p.Stats,
						ParentStats:  pipelineData.Stats,
						Index:        i,
						Type:         processorType,
						Name:         strconv.Itoa(i) + "_" + processorType,
					}
					if processorDef != nil {
						ifCond := processorDef.If != nil
						ds.Conditional = &ifCond
						ds.Definition = string(processorDef.raw)
						if processorDef.Tag != nil {
							ds.Tag = *processorDef.Tag
							ds.Name += "_" + strings.ReplaceAll(strings.ToLower(*processorDef.Tag), " ", "_")
						}
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

func readPipelines(dir string) (map[string]*IngestPipeline, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var pipelines map[string]*IngestPipeline
	dec := json.NewDecoder(f)
	if err = dec.Decode(&pipelines); err != nil {
		return nil, err
	}

	return pipelines, nil
}
