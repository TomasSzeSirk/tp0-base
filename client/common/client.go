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
const READ_BUFFER_SIZE = 1024

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
		return err
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

	if c.receiveWinners() != nil {
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
	file, err := os.Open(FILE_PATH)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	batch := make([]*Bet, 0, c.config.BatchMaxAmount)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			if len(batch) > 0 {
				if err := c.sendBatch(batch); err != nil {
					return err
				}
				c.conn.Read(make([]byte, 1))
			}
			break
		}

		if err != nil {
			return err
		}

		if len(record) != 5 {
			return errors.New("NotSameNumberOfFields")
		}

		bet := &Bet{
			Agency:    c.config.ID,
			FirstName: record[0],
			LastName:  record[1],
			Document:  record[2],
			Birthdate: record[3],
			Number:    record[4],
		}

		batch = append(batch, bet)

		if len(batch) == c.config.BatchMaxAmount {
			if err := c.sendBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
			c.conn.Read(make([]byte, 1))
		}
	}

	log.Infof("action: apuestas_enviadas | result: success")
	c.conn.Write([]byte("E"))

	return nil
}

func (c *Client) sendBatch(batch []*Bet) error {

	batch_size := c.BatchSizeToBytes(len(batch))
	c.conn.Write(batch_size)

	for _, bet := range batch {
		data := bet.toBytes()
		length := len(data)

		for length > 0 {
			n, err := c.conn.Write(data)
			if err != nil {
				return err
			}

			data = data[n:]
			length -= n
		}

	}

	return nil
}

func (c *Client) BatchSizeToBytes(n int) []byte {
	return []byte{
		byte((n >> 8) & 0xFF),
		byte(n & 0xFF),
	}
}

func (c *Client) receiveWinners() error {
	err := c.sendWinnersRequest()
	if err != nil {
		log.Criticalf(`action: enviar_pedido | result: fail | error: %v`, err)
		return err
	}

	log.Infof(`action: enviar_pedido | result: success`)

	buffer := make([]byte, 1024)

	_, err = c.conn.Read(buffer[:2])

	if err != nil {
		log.Criticalf(`action: consulta_ganadores | result: fail | error: %v`, err)
		return err
	}

	count := int(buffer[0])<<8 | int(buffer[1])
	winners := make([]string, 0, count)

	for i := 0; i < count; i++ {
		total := 0
		for total < 4 {
			n, err := c.conn.Read(buffer[total:4])
			if err != nil {
				log.Criticalf(`action: consulta_ganadores | result: fail | error: %v`, err)
				return err
			}
			total += n
		}
		winners = append(winners, string(buffer[:4]))
	}

	log.Infof(`action: consulta_ganadores | result: success | cant_ganadores: %v`, len(winners))
	return nil
}

func (c *Client) sendWinnersRequest() error {
	msg := []byte(c.config.ID)
	_, err := c.conn.Write(msg)
	if err != nil {
		return err
	}
	return nil
}
