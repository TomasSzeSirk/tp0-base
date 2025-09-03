package common

import (
	"bufio"
	"encoding/csv"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

const FILE_PATH = "./agency.csv"

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	BatchMaxAmount int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		c.HandleSIGTERM(sigs)
		os.Exit(0)
	}()

	if c.createClientSocket() != nil {
		close(sigs)
		os.Exit(1)
	}

	if c.sendFileInBatches() != nil {
		close(sigs)
		os.Exit(1)
	}
}

func (c *Client) HandleSIGTERM(sigs chan os.Signal) {
	if c.conn != nil {
		err := c.conn.Close()
		if err == nil {
			log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
		}
	}

	if sigs != nil {
		close(sigs)
		log.Infof("action: close_client | result: success | client_id: %v", c.config.ID)
	}
}

func (c *Client) sendFileInBatches() error {
	betFields := []string{"agency", "first_name", "last_name", "document", "birthdate", "number"}
	file, err := os.Open(FILE_PATH)
	if err != nil {
		return err
	}

	reader := csv.NewReader(bufio.NewReader(file))

	if _, err := reader.Read(); err != nil {
		return err
	}

	batch := make([]*Bet, c.config.BatchMaxAmount)

	for {
		record, err := reader.Read()

		if err == io.EOF {
			if len(batch) > 0 {
				if err := c.sendBatch(batch); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return err
		}

		if len(record) != len(betFields) {
			return errors.New("NotSameNumberOfFields")
		}

		bet := &Bet{
			Agency:    record[0],
			FirstName: record[1],
			LastName:  record[2],
			Document:  record[3],
			Birthdate: record[4],
			Number:    record[5],
		}

		batch = append(batch, bet)

		if len(batch) == c.config.BatchMaxAmount {
			if err := c.sendBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	c.conn.Read(make([]byte, 1))

	log.Infof("action: apuestas_enviadas | result: success")

	return nil
}

func (c *Client) sendBatch(batch []*Bet) error {
	for _, bet := range batch {
		data := bet.toBytes()
		length := len(data)

		for length > 0 {
			n, err := c.conn.Write(data)
			if errors.Is(err, io.ErrClosedPipe) {
				return err
			}

			data = data[n:]
			length -= n
		}

	}

	return nil
}
