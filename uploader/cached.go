package uploader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	cacheStart            = time.Now()
	cache                 = NewCMap()
	cacheTableCount uint8 = 0 // ugly global counter but it shouldn't hurt
)

func init() {
	go cache.ExpireWorker(context.Background(), 1*time.Hour, nil)
}

func timeRel() uint32 {
	return uint32(time.Now().Sub(cacheStart).Seconds())
}

type DebugCacheDumper interface {
	CacheDump(io.Writer)
}

type cached struct {
	*Base
	//existsCache CMap // store known keys and don't load it to clickhouse tree
	parser  func(filename string, out io.Writer) (map[string]bool, error)
	expired uint32 // atomic counter
	id      uint8
}

func newCached(base *Base) *cached {
	u := &cached{Base: base}
	u.Base.handler = u.upload
	//u.existsCache = NewCMap()
	u.query = fmt.Sprintf("%s (Date, Level, Path, Version)", u.config.TableName)
	u.id = cacheTableCount
	cacheTableCount++
	return u
}

func (u *cached) Stat(send func(metric string, value float64)) {
	u.Base.Stat(send)

	//send("cacheSize", float64(u.existsCache.Count()))

	expired := atomic.LoadUint32(&u.expired)
	atomic.AddUint32(&u.expired, -expired)
	send("expired", float64(expired))
}

func (u *cached) Start() error {
	err := u.Base.Start()
	if err != nil {
		return err
	}

	// if u.config.CacheTTL.Value() != 0 {
	// 	u.Go(func(ctx context.Context) {
	// 		u.existsCache.ExpireWorker(ctx, u.config.CacheTTL.Value(), &u.expired)
	// 	})
	// }

	return nil
}

func (u *cached) Reset() {
	//	u.existsCache.Clear()
}

func (u *cached) upload(ctx context.Context, logger *zap.Logger, filename string) error {
	pipeReader, pipeWriter := io.Pipe()
	writer := bufio.NewWriter(pipeWriter)
	startTime := timeRel()

	uploadResult := make(chan error, 1)

	u.Go(func(ctx context.Context) {
		err := u.insertRowBinary(
			u.query,
			pipeReader,
		)
		uploadResult <- err
		if err != nil {
			pipeReader.CloseWithError(err)
		}
	})

	newSeries, err := u.parser(filename, writer)
	_ = newSeries
	if err == nil {
		err = writer.Flush()
	}
	pipeWriter.CloseWithError(err)

	var uploadErr error

	select {
	case uploadErr = <-uploadResult:
		// pass
	case <-ctx.Done():
		return fmt.Errorf("upload aborted")
	}

	if err != nil {
		return err
	}

	if uploadErr != nil {
		return uploadErr
	}

	// commit new series
	cache.Merge(u.id, newSeries, startTime)

	return nil
}
