package cache_server

import (
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrCacheMiss = errors.New("cache miss hit")
)

type CacheKeyType int

const (
	CacheKeyTypeNone CacheKeyType = iota
	CacheKeyTypeSLC
	CacheKeyTypeLLC
)

func (typ CacheKeyType) String() string {
	switch typ {
	case CacheKeyTypeSLC:
		return "slc"
	case CacheKeyTypeLLC:
		return "llc"
	}
	return "none"
}

func (typ CacheKeyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, typ.String())), nil
}

func (typ *CacheKeyType) UnmarshalJSON(bytes []byte) error {
	switch string(bytes) {
	case `"slc"`:
		*typ = CacheKeyTypeSLC
	case `"llc"`:
		*typ = CacheKeyTypeLLC
	default:
		*typ = CacheKeyTypeNone
	}
	return nil
}

type CacheKey interface {
	String() string
	Hash() uint32
	Addr() net.Addr
	LockKey() CacheKey
	Type() CacheKeyType
}

type CacheServer interface {
	Get(CacheKey) (*CacheGetResponse, error)
	GetMulti([]CacheKey) (*Iterator, error)
	Set(*CacheStoreRequest) error
	Add(CacheKey, []byte, time.Duration) error
	Delete(CacheKey) error
	Flush() error
}

type CacheGetResponse struct {
	Value []byte
	Flags uint32
	CasID uint64
}

type CacheStoreRequest struct {
	Key        CacheKey
	Value      []byte
	CasID      uint64
	Expiration time.Duration
}

type Iterator struct {
	currentIndex int
	keys         []CacheKey
	values       []*CacheGetResponse
	errs         []error
}

func (i *Iterator) SetContent(idx int, res *CacheGetResponse) {
	i.values[idx] = res
}

func (i *Iterator) SetError(idx int, err error) {
	i.errs[idx] = err
}

func NewIterator(keys []CacheKey) *Iterator {
	return &Iterator{
		currentIndex: -1,
		keys:         keys,
		values:       make([]*CacheGetResponse, len(keys)),
		errs:         make([]error, len(keys)),
	}
}

func (i *Iterator) Next() bool {
	if i.currentIndex < len(i.keys)-1 {
		i.currentIndex++
		return true
	}
	return false
}

func (i *Iterator) Key() CacheKey {
	return i.keys[i.currentIndex]
}

func (i *Iterator) Content() *CacheGetResponse {
	return i.values[i.currentIndex]
}

func (i *Iterator) Error() error {
	return i.errs[i.currentIndex]
}
