package storage

import "github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/common"

type Storage interface {
	SaveDocument(doc common.Document) error
}
