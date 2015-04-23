package main

import (
	"encoding/json"
	"fmt"
	"github.com/evandigby/rtb"
	"github.com/evandigby/rtb/amqp"
	"github.com/evandigby/rtb/inmemory"
	rtbr "github.com/evandigby/rtb/redis"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Temporary in memory state for testing
type RtbApp struct {
	logger           rtb.BidLogProducer
	bidLoggerChannel chan *rtb.BidLogItem // Limits the number of connections to loggers

	domain       string
	redisNetwork string
	redisAddress string

	campaignProvider rtb.CampaignProvider
	banker           rtb.Banker
	pacer            rtb.Pacer
	updateTicker     *time.Ticker
	accountBids      chan *rtb.BidResponse
	bidTotals        map[int64]int64
}

type RtbAppOptions struct {
	Domain                   string
	PeriodicUpdates          bool
	UpdateFrequencyInSeconds int
	PacerOptions             struct {
		UsePacer             bool
		TimeSegmentInSeconds int
	}
	DataAccessOptions struct {
		Network            string
		Address            string
		ConnectionPoolSize int
	}
	LoggingOptions struct {
		AmqpAddress     string
		File            *os.File
		Verbose         bool
		FullBidResponse bool
	}
}

func (app *RtbApp) CreateDefaultCampaigns() {
	app.campaignProvider.CreateCampaign(100101, rtb.CpmToMicroCents(0.32), rtb.DollarsToMicroCents(35.50), []rtb.Target{rtb.Target{Type: rtb.Placement, Value: "Words With Friends 2 iPad"}})
	app.campaignProvider.CreateCampaign(100102, rtb.CpmToMicroCents(0.04), rtb.DollarsToMicroCents(5.25), []rtb.Target{rtb.Target{Type: rtb.CreativeSize, Value: "728x90"}})
	app.campaignProvider.CreateCampaign(100103, rtb.CpmToMicroCents(0.32), rtb.DollarsToMicroCents(15.00), []rtb.Target{rtb.Target{Type: rtb.Country, Value: "USA"}})
	app.campaignProvider.CreateCampaign(100104, rtb.CpmToMicroCents(0.15), rtb.DollarsToMicroCents(22.00), []rtb.Target{rtb.Target{Type: rtb.OS, Value: "Android"}})
	app.campaignProvider.CreateCampaign(100105, rtb.CpmToMicroCents(0.02), rtb.DollarsToMicroCents(2.25), []rtb.Target{rtb.Target{Type: rtb.Country, Value: "CAN"}})
}

func (app *RtbApp) bidRequestForHttpRequest(r *http.Request) *rtb.BidRequest {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		panic(err)
	}

	var bidRequest rtb.BidRequest

	err = json.Unmarshal(body, &bidRequest)

	return &bidRequest
}

func (app *RtbApp) BidRequestHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now().UTC().UnixNano()
	bidRequest := app.bidRequestForHttpRequest(r)

	bidder := inmemory.NewBidRequestBidder(bidRequest, app.campaignProvider, app.pacer, time.Now().UTC())

	b, remainingDailyBudgetsInMicroCents, err := bidder.Bid()

	if err == nil && b != nil {
		js, err := json.Marshal(b)

		if err == nil {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)

			// Push the bid onto our stats process
			app.accountBids <- b
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	} else {
		w.WriteHeader(http.StatusNoContent)
	}

	if app.logger != nil {
		app.bidLoggerChannel <- &rtb.BidLogItem{Domain: app.domain, BidRequest: bidRequest, BidResponse: b, RemainingDailyBudgetsInMicroCents: remainingDailyBudgetsInMicroCents, StartTimestampInNanoseconds: startTime, EndTimestampInNanoseconds: time.Now().UTC().UnixNano()}
	}

}

func (app *RtbApp) PrintCampaigns(campaigns []int64) {
	fmt.Println()
	for _, val := range campaigns {
		c := app.campaignProvider.ReadCampaign(val)
		remaining := app.banker.RemainingDailyBudgetInMicroCents(c.Id())
		fmt.Printf("Campaign Id: %v / CPM: $%v / Remaining Daily Budget: $%v / Total Bids: %v\n", c.Id(), rtb.MicroCentsToDollarsRounded(c.BidCpmInMicroCents(), 2), rtb.MicroCentsToDollarsRounded(remaining, 5), app.bidTotals[c.Id()])
	}
}

func (app *RtbApp) PrintUpdates() {
	for {
		<-app.updateTicker.C
		app.PrintCampaigns(app.campaignProvider.ListCampaigns())
	}
}

func (app *RtbApp) BidTotalUpdater() {
	for {
		rsp := <-app.accountBids

		if rsp.Seatbid != nil {
			for _, rsp := range rsp.Seatbid {
				if rsp.Bid != nil {
					for _, bid := range rsp.Bid {
						campaignId, _ := strconv.ParseInt(bid.Cid, 10, 64)
						total := app.bidTotals[campaignId] + 1
						app.bidTotals[campaignId] = total
					}
				}
			}
		}
	}
}

func (app *RtbApp) LogBids() {
	for {
		logItem := <-app.bidLoggerChannel

		app.logger.LogItem(logItem)
	}
}

func NewRtbApp(options RtbAppOptions) *RtbApp {
	app := new(RtbApp)

	app.domain = options.Domain
	app.redisAddress = options.DataAccessOptions.Address
	app.redisNetwork = options.DataAccessOptions.Network

	if options.LoggingOptions.Verbose || options.LoggingOptions.FullBidResponse {
		if options.LoggingOptions.File != nil {
			app.logger = inmemory.NewFileBidLogger(options.LoggingOptions.File, options.LoggingOptions.FullBidResponse)
		} else {
			app.logger = amqp.NewAmqpBidLogger(options.LoggingOptions.AmqpAddress, options.Domain)
		}

		app.bidLoggerChannel = make(chan *rtb.BidLogItem)
		go app.LogBids()
	}

	da := rtbr.NewRedisDataAccess(app.redisNetwork, app.redisAddress, app.domain, options.DataAccessOptions.ConnectionPoolSize)
	app.banker = rtbr.NewRedisBanker(da)

	app.campaignProvider = rtbr.NewRedisCampaignProvider(da, app.banker)

	if options.PacerOptions.UsePacer {
		app.pacer = rtbr.NewRedisPacer(app.campaignProvider, da, app.banker, time.Duration(options.PacerOptions.TimeSegmentInSeconds)*time.Second)
	}
	app.CreateDefaultCampaigns()

	if options.PeriodicUpdates {
		app.updateTicker = time.NewTicker(time.Duration(options.UpdateFrequencyInSeconds) * time.Second)

		go app.BidTotalUpdater()
		go app.PrintUpdates()
	}

	app.accountBids = make(chan *rtb.BidResponse)
	app.bidTotals = make(map[int64]int64)

	return app
}
