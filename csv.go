package cbcsv

import (
	"encoding/csv"
	"errors"
	"github.com/jszwec/csvutil"
	"io"
	"os"
	"time"
)

var (
	ErrNotInReadMode  = errors.New("csv is not in read mode")
	ErrNotInWriteMode = errors.New("csv is not in write mode")
)

type Csv[T any] struct {
	options *csvOptions

	writer    io.WriteCloser
	csvWriter *csv.Writer
	encoder   *csvutil.Encoder

	reader    io.ReadCloser
	csvReader *csv.Reader
	decoder   *csvutil.Decoder

	writerChan chan T
	writeErrs  []error
}

type csvOptions struct {
	write bool
	read  bool

	writer io.WriteCloser
	reader io.ReadCloser

	filePath     string
	truncateFile bool
	appendFile   bool

	missingHeader bool
	skipRows      int
	skipErrors    bool
}

type CsvOption func(o *csvOptions)

func WithWriter(writer io.WriteCloser) CsvOption {
	return func(o *csvOptions) {
		o.write = true
		o.writer = writer
	}
}

func WithReader(reader io.ReadCloser) CsvOption {
	return func(o *csvOptions) {
		o.read = true
		o.reader = reader
	}
}

func WithFilePath(filePath string) CsvOption {
	return func(o *csvOptions) {
		o.filePath = filePath
	}
}

func WithTruncatedFile() CsvOption {
	return func(o *csvOptions) {
		o.truncateFile = true
	}
}

func WithAppendFile() CsvOption {
	return func(o *csvOptions) {
		o.appendFile = true
	}
}

func WithWrite() CsvOption {
	return func(o *csvOptions) {
		o.write = true
	}
}

func WithRead() CsvOption {
	return func(o *csvOptions) {
		o.read = true
	}
}

func WithSkipRows(rows int) CsvOption {
	return func(o *csvOptions) {
		o.skipRows = rows
	}
}

func WithMissingHeader() CsvOption {
	return func(o *csvOptions) {
		o.missingHeader = true
	}
}

func WithSkipErrors() CsvOption {
	return func(o *csvOptions) {
		o.skipErrors = true
	}
}

func NewCsv[T any](opts ...CsvOption) (c *Csv[T], e error) {
	options := &csvOptions{}
	for _, opt := range opts {
		opt(options)
	}

	c = &Csv[T]{
		options: options,
	}

	if options.filePath != "" {
		if options.write {
			if options.truncateFile {
				if c.writer, e = os.Create(options.filePath); e != nil {
					return nil, e
				}
			} else if options.appendFile {
				if c.writer, e = os.OpenFile(options.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); e != nil {
					return nil, e
				}
			}
		}
		if options.read {
			if c.reader, e = os.Open(options.filePath); e != nil {
				return nil, e
			}
		}
	}
	if options.writer != nil {
		c.writer = options.writer
	}
	if options.reader != nil {
		c.reader = options.reader
	}

	var headers []string

	if options.missingHeader {
		headers, e = csvutil.Header(new(T), "csv")
		if e != nil {
			return nil, e
		}
	}

	if c.writer != nil {
		c.csvWriter = csv.NewWriter(c.writer)
		c.encoder = csvutil.NewEncoder(c.csvWriter)
		c.encoder.SetHeader(headers)
		c.writerChan = make(chan T, 1000)
		go func() {
			for item := range c.writerChan {
				if e := c.write(item); e != nil {
					c.writeErrs = append(c.writeErrs, e)
				}
			}
		}()
	}
	if c.reader != nil {
		c.csvReader = csv.NewReader(c.reader)
		if c.decoder, e = csvutil.NewDecoder(c.csvReader, headers...); e != nil {
			return nil, e
		}
	}

	return c, nil
}

func (c *Csv[T]) Write(item T) error {
	if c.writer != nil {
		c.writerChan <- item
	} else {
		c.writeErrs = append(c.writeErrs, ErrNotInWriteMode)
	}
	if len(c.writeErrs) > 0 {
		e := c.writeErrs[0]
		c.writeErrs = c.writeErrs[:0]
		return e
	}
	return nil
}

func (c *Csv[T]) write(item T) error {
	if e := c.encoder.Encode(item); e != nil {
		return e
	}
	c.csvWriter.Flush()
	return c.csvWriter.Error()
}

func (c *Csv[T]) Read() (T, error) {
	var item T
	if c.reader != nil {
		if e := c.decoder.Decode(&item); e != nil {
			return item, e
		}
		return item, nil
	}
	return item, ErrNotInReadMode
}

type ConcurrentWaitGroup interface {
	Add()
	Done()
	Wait()
}

func (c *Csv[T]) Range(fn func(offset int, item T) error, wg ConcurrentWaitGroup) error {
	if c.reader != nil {
		var concurrentErr error
		var rowsRead int
		for {
			if concurrentErr != nil {
				return concurrentErr
			}
			item, e := c.Read()
			if e != nil {
				if errors.Is(e, io.EOF) {
					break
				}
				if c.options.skipErrors {
					continue
				}
				return e
			}
			rowsRead++
			if rowsRead < c.options.skipRows {
				continue
			}
			if wg != nil {
				wg.Add()
				go func() {
					defer wg.Done()
					concurrentErr = fn(rowsRead, item)
				}()
			} else {
				if e := fn(rowsRead, item); e != nil {
					return e
				}
			}
		}
		if wg != nil {
			wg.Wait()
			return concurrentErr
		}
		return nil
	}
	return ErrNotInReadMode
}

func (c *Csv[T]) Close() (errs []error) {
	if c.writer != nil {
		for {
			if len(c.writerChan) == 0 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		if e := c.writer.Close(); e != nil {
			errs = append(errs, e)
		}
	} else if c.reader != nil {
		if e := c.reader.Close(); e != nil {
			errs = append(errs, e)
		}
	}
	return errs
}
