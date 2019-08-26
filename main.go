package main

import (
	"os"
	"time"

	"github.com/go-numb/go-bitbank/realtime"
	"github.com/labstack/gommon/log"
	"gopkg.in/mgo.v2"
)

const (
	DBDIAL   = "localhost:27017"
	DBPREFIX = "test_"

	LOGOUTPUTLEVEL = log.INFO
)

func init() {
	// setup Logger
	f, err := os.OpenFile("server.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	log.SetLevel(LOGOUTPUTLEVEL)
	log.SetOutput(f)
}

func main() {
	done := make(chan struct{})

	sec, err := mgo.Dial(DBDIAL)
	if err != nil {
		log.Fatal(err)
	}
	defer sec.Close()

	btc := sec.DB(DBPREFIX + "bb").C("btc")
	if err != nil {
		log.Fatal(err)
	}

	go read(btc)

	<-done
}

func read(db *mgo.Collection) {
Reconnect:
	wsError := make(chan error)

	c, err := realtime.Connect(false)
	if err != nil {
		log.Error(err)
	}

	channels := []string{
		realtime.ChTransactions,
	}
	pairs := []string{
		realtime.BTCJPY,
		// realtime.XRPJPY,
		// realtime.ETHBTC,
	}

	// 購読チャンネルをbitbankに通知し購読開始
	go c.Realtime(channels, pairs)

	for {
		select {
		case v := <-c.Subscriber:
			switch v.Types {

			case realtime.TypeTransactions:
				go func() {
					for _, value := range v.Transactions {
						if err := db.Insert(value); err != nil {
							log.Error(err)
						}
					}
				}()

			case realtime.TypeError:
				wsError <- v.Error
			}
		}
	}

	err = <-wsError
	log.Error(err)
	c.Close()
	time.Sleep(3 * time.Second)
	goto Reconnect
}
