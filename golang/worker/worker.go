package worker

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/slyrz/warc"

	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/common"
	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/commoncrawl"
	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/rabbitmq"
	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/storage"
)

var (
	batchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_batches",
		Help: "Number of consumed batches",
	})

	processedWARCFilesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_processed_warc_files",
			Help: "Number of processed WARC files",
		},
		[]string{"result"},
	)
)

const (
	processedWARCFilesResultDownloaded = "downloaded"
	processedWARCFilesResultFiltered   = "filtered"
	processedWARCFilesResultStored     = "stored"
)

func init() {
	prometheus.MustRegister(batchCounter)
	prometheus.MustRegister(processedWARCFilesCounter)
}

// extractText extracts text content from HTML using goquery
func extractText(htmlContent []byte) string {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlContent))
	if err != nil {
		log.Printf("Failed to parse HTML: %v", err)
		// TODO pheymann: Why not return an explicit error here?
		return ""
	}

	doc.Find("script, style").Each(func(i int, s *goquery.Selection) {
		s.Remove()
	})

	text := doc.Text()
	text = strings.Join(strings.Fields(text), " ")

	return text
}

func ProcessBatch(
	downloader commoncrawl.Downloader,
	delivery amqp.Delivery,
	docStorage storage.Storage,
) error {
	var batch []common.URL
	if err := json.Unmarshal(delivery.Body, &batch); err != nil {
		return fmt.Errorf("failed to unmarshal batch: %w", err)
	}

	for _, item := range batch {
		// TODO pheymann: Maybe it makes sense to extract required fields already in the batcher?
		// That way we would have certainty that they exist and are valid when data reaches the
		// worker.
		offset, err := strconv.Atoi(item.Metadata["offset"].(string))
		if err != nil {
			return fmt.Errorf("failed to parse offset: %w", err)
		}

		length, err := strconv.Atoi(item.Metadata["length"].(string))
		if err != nil {
			return fmt.Errorf("failed to parse length: %w", err)
		}

		data, err := downloader.DownloadAndUnzip(
			item.Metadata["filename"].(string),
			offset,
			length,
		)
		if err != nil {
			return fmt.Errorf("failed to download and unzip: %w", err)
		}

		reader, err := warc.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create WARC reader: %w", err)
		}
		defer reader.Close()

		processedWARCFilesCounter.WithLabelValues(processedWARCFilesResultDownloaded).Add(float64(len(batch)))

		// TODO pheymann: Why not just log errors and continue with the batch?
		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if err == io.EOF {
					break
				}

				return fmt.Errorf("failed to read WARC record: %w", err)
			}
			if record.Header.Get("WARC-Type") != "response" {
				processedWARCFilesCounter.WithLabelValues(processedWARCFilesResultFiltered).Inc()
				continue
			}

			content, err := io.ReadAll(record.Content)
			if err != nil {
				return fmt.Errorf("failed to read WARC record content: %w", err)
			}

			// Find the start of HTML content (after headers)
			htmlStart := bytes.Index(content, []byte("\r\n\r\n"))
			if htmlStart == -1 {
				htmlStart = bytes.Index(content, []byte("\n\n"))
			}
			if htmlStart == -1 {
				processedWARCFilesCounter.WithLabelValues(processedWARCFilesResultFiltered).Inc()
				log.Printf("Could not find HTML content start for URL %s", item.SurtURL)
				continue
			}

			text := extractText(content[htmlStart+4:])
			if text != "" {
				log.Printf("storing document: %s", item.SurtURL)
				doc := common.Document{
					URL:  item,
					Text: text,
				}
				if err := docStorage.SaveDocument(doc); err != nil {
					return fmt.Errorf("failed to save document %s: %w", item.SurtURL, err)
				}

				processedWARCFilesCounter.WithLabelValues(processedWARCFilesResultStored).Inc()
			}
		}
	}

	batchCounter.Inc()
	return nil
}

// TODO pheymann: Same as with the batcher, I would move the initialization to the main function.
func Run() error {
	rabbitMQPort := flag.Int("rabbitmq-port", 55005, "Port to connect to RabbitMQ")
	flag.Parse()

	if rabbitMQPort == nil || *rabbitMQPort <= 0 {
		return fmt.Errorf("rabbitmq-port is required")
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9001", nil); err != nil {
			log.Printf("Failed to start metrics server: %v", err)
		}
	}()

	downloader := commoncrawl.NewCCDownloader(commoncrawl.BaseURL)

	channel, err := rabbitmq.NewRabbitMQChannel(*rabbitMQPort)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}
	defer channel.Close()

	if err := channel.SetQoS(1, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	storage, err := storage.NewMiniOStorage(
		"127.0.0.1:9005",
		"minioadmin",
		"minioadmin",
		"web-crawler-documents",
	)
	if err != nil {
		return fmt.Errorf("failed to create storage connection: %w", err)
	}

	if err := channel.BasicConsume(rabbitmq.QueueName, func(delivery amqp.Delivery) {
		if err := ProcessBatch(downloader, delivery, storage); err != nil {
			log.Printf("Failed to process batch: %v", err)
			return
		}
		if err := channel.BasicAck(delivery.DeliveryTag, false); err != nil {
			log.Printf("Failed to acknowledge message: %v", err)
		}
	}); err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.Printf("Started consuming")

	select {}
}
