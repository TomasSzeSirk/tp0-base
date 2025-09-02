package common

import (
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
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

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed

	if c.createClientSocket() != nil {
		close(sigs)
		os.Exit(1)
	}

	bet := readBetFromEnv()
	betBytes := bet.toBytes()
	length := len(betBytes)

	for length > 0 {
		n, err := c.conn.Write(betBytes)
		if errors.Is(err, io.ErrClosedPipe) {
			log.Criticalf(`action: apuesta_enviada | result: fail | dni: %v | numero: %v | error: %v`, bet.Document, bet.Number, err)
			close(sigs)
			os.Exit(1)
		}

		betBytes = betBytes[n:]
		length -= n
	}

	log.Infof(`action: apuesta_enviada | result: success | dni: %v | numero: %v`, bet.Document, bet.Number)
}

func readBetFromEnv() *Bet {
	return &Bet{
		Agency:    os.Getenv("CLI-ID"),
		FirstName: os.Getenv("NOMBRE"),
		LastName:  os.Getenv("APELLIDO"),
		Document:  os.Getenv("DOCUMENTO"),
		Birthdate: os.Getenv("NACIMIENTO"),
		Number:    os.Getenv("NUMERO"),
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
