package batcher

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/commoncrawl"
	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/rabbitmq"
)

const (
	batchSize = 50

	// Records have the following format:
	//
	// 0 - SURL, 1 - timestamp, 2 - file name, 3 - startOffset, 4 - length, 5 - index
	//
	// 0,100,22,165)/ 20240722120756   cdx-00000.gz    0       188224  1
	// 101,141,199,66)/robots.txt 20240714155331       cdx-00000.gz    188224  178351  2
	// 104,223,1,100)/ 20240714230020  cdx-00000.gz    366575  178055  3
	// 107,128,254,23)/sites.asp?domain=hydrogenheaters.com 20240725183414     cdx-00000.gz    544630  181599  4
	// 109,77,250,142)/url?q=https://batmanapollo.ru 20240722133024    cdx-00000.gz    726229  181656  5
	recordSURLIndex        = 0
	recordFileNameIndex    = 1
	recordStartOffsetIndex = 2
	recordLengthIndex      = 3
	recordIndexIndex       = 4

	expectedNumberOfIndexFields = 3
	indexSURLIndex              = 0
	indexTimestampIndex         = 1
	indexCrawlMetadataIndex     = 2
)

var (
	batchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "batcher_batches",
		Help: "Number of published batches",
	})

	downloadedURLArchivessCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "batcher_downloaded_url_archives",
		Help: "Number of downloaded URL archives",
	})

	processedURLsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batcher_processed_urls",
			Help: "Number of processed URLs",
		},
		[]string{"result"},
	)
)

const (
	processedURLsResultFiltered = "filtered"
	processedURLsResultQueued   = "queued"
)

func init() {
	prometheus.MustRegister(batchCounter)
	prometheus.MustRegister(downloadedURLArchivessCounter)
	prometheus.MustRegister(processedURLsCounter)
}

func ProcessIndex(filename string, mq rabbitmq.MessageQueueChannel, downloader commoncrawl.Downloader) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open index file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'         // Use tab as delimiter
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	var foundURLs []common.URL
	count := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("Error reading record: %v", err)
			continue
		}

		count++

		if len(record) < 4 {
			log.Printf("Invalid record length: %v", record)
			continue
		}

		startOffset, err := strconv.Atoi(record[recordStartOffsetIndex])
		if err != nil {
			log.Printf("Failed to parse start offset: %v", err)
			continue
		}

		length, err := strconv.Atoi(record[recordLengthIndex])
		if err != nil {
			log.Printf("Failed to parse length: %v", err)
			continue
		}

		cdxPath := fmt.Sprintf("%s/%s", commoncrawl.CrawlPath, record[recordFileNameIndex])
		content, err := downloader.DownloadAndUnzip(cdxPath, startOffset, length)
		if err != nil {
			log.Printf("Failed to download and unzip: %v", err)
			continue
		}

		downloadedURLArchivessCounter.Inc()

		// Parse the downloaded content into URLs
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
				continue
			}

			// The format of the metadata is:
			//
			// 0,100,22,165)/
			// 20240722120756
			// {
			//     "url": "http://165.22.100.0/",
			//     "mime": "text/html",
			//     "mime-detected": "text/html",
			//     "status": "301",
			//     "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R",
			//     "length": "689",
			//     "offset": "3499",
			//     "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz",
			//     "redirect": "https://157.245.55.71/"
			// }
			parts := strings.SplitN(line, " ", expectedNumberOfIndexFields)
			if len(parts) != expectedNumberOfIndexFields {
				processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
				continue
			}

			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(parts[indexCrawlMetadataIndex]), &metadata); err != nil {
				processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
				continue
			}

			url := common.URL{
				SurtURL:   parts[indexSURLIndex],
				Timestamp: parts[indexTimestampIndex],
				Metadata:  metadata,
			}

			// only continue if the URL references a page in English
			if languages, ok := url.Metadata["languages"].(string); ok {
				hasEnglish := false
				// Split the comma-separated string into individual languages
				langList := strings.Split(languages, ",")
				for _, lang := range langList {
					if strings.TrimSpace(lang) == "eng" {
						hasEnglish = true
						break
					}
				}

				if !hasEnglish {
					processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
					continue
				}
			} else {
				processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
				continue
			}

			if status, ok := url.Metadata["status"].(string); !ok || status != "200" {
				processedURLsCounter.WithLabelValues(processedURLsResultFiltered).Inc()
				continue
			}

			foundURLs = append(foundURLs, url)

			if len(foundURLs) >= batchSize {
				if err := publishBatch(mq, foundURLs); err != nil {
					log.Printf("Failed to publish batch: %v", err)
				}
				foundURLs = nil

				processedURLsCounter.WithLabelValues(processedURLsResultQueued).Add(float64(len(foundURLs)))
			}
		}
	}

	if len(foundURLs) > 0 {
		if err := publishBatch(mq, foundURLs); err != nil {
			log.Printf("Failed to publish batch: %v", err)
		}
		processedURLsCounter.WithLabelValues(processedURLsResultQueued).Add(float64(len(foundURLs)))
	}

	return nil
}

func publishBatch(channel rabbitmq.MessageQueueChannel, batch []common.URL) error {
	log.Printf("Pushing batch of size %d", len(batch))
	if err := rabbitmq.PublishBatch(channel, batch); err != nil {
		return err
	}
	batchCounter.Inc()
	return nil
}

// TODO pheymann: I would move this whole initialization block to the main function. Explanation TBD.
func Run() error {
	clusterIdxFilename := flag.String("cluster-idx-filename", "", "Path to the cluster index file")
	rabbitMQPort := flag.Int("rabbitmq-port", 55005, "Port to connect to RabbitMQ")
	flag.Parse()

	if *clusterIdxFilename == "" {
		return fmt.Errorf("cluster-idx-filename is required")
	}

	if rabbitMQPort == nil || *rabbitMQPort <= 0 {
		return fmt.Errorf("rabbitmq-port is required")
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9000", nil); err != nil {
			log.Printf("Failed to start metrics server: %v", err)
		}
	}()

	channel, err := rabbitmq.NewRabbitMQChannel(*rabbitMQPort)
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer channel.Close()

	downloader := commoncrawl.NewCCDownloader(commoncrawl.BaseURL)

	return ProcessIndex(*clusterIdxFilename, channel, downloader)
}
