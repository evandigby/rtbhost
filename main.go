package main

import (
	"flag"
	"net/http"
	"os"
)

func main() {
	var address string
	var endpoint string
	var options RtbAppOptions
	var logToFile bool
	var fileName string

	flag.StringVar(&address, "address", "localhost:8000", "Address of this instance, including port.")
	flag.StringVar(&endpoint, "endpoint", "/", "Endpoint for.")
	flag.StringVar(&options.Domain, "appdomain", "rtb-interview", "App domain used to uniquely identify this application. All server instances of the same application should match.")
	flag.StringVar(&options.DataAccessOptions.Network, "redisnetwork", "tcp", "Redis network.")
	flag.StringVar(&options.DataAccessOptions.Address, "redisaddress", "localhost:6379", "Redis network.")
	flag.IntVar(&options.DataAccessOptions.ConnectionPoolSize, "redispoolsize", 100, "Maximum number of concurrent redis connections from this host.")
	flag.BoolVar(&options.PacerOptions.UsePacer, "usepacer", false, "Use the simple time segmented pacer.")
	flag.IntVar(&options.PacerOptions.TimeSegmentInSeconds, "pacersegment", 300, "Number of segments to pace the bids over.")
	flag.BoolVar(&logToFile, "logtofile", true, "Log to file (overrides amqp options).")
	flag.StringVar(&fileName, "logfilename", "", "File to log to (defaults to stdout).")
	flag.StringVar(&options.LoggingOptions.AmqpAddress, "amqpaddress", "amqp://guest:guest@localhost:5672/", "AMQP Logging address.")
	flag.BoolVar(&options.LoggingOptions.Verbose, "logverbose", false, "Log each request.")
	flag.BoolVar(&options.LoggingOptions.FullBidResponse, "logfullbidresponse", false, "Log full bid response (json).")
	flag.BoolVar(&options.PeriodicUpdates, "periodicupdates", true, "Print periodic updates on campaigns to screen.")
	flag.IntVar(&options.UpdateFrequencyInSeconds, "updatefrequency", 5, "Update period in seconds.")

	flag.Parse()

	if logToFile {
		options.LoggingOptions.AmqpAddress = ""
		if fileName == "" {
			options.LoggingOptions.File = os.Stdout
		} else {
			file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

			if err == nil {
				defer file.Close()
				options.LoggingOptions.File = file
			}
		}
	}

	app := NewRtbApp(options)

	http.HandleFunc(endpoint, app.BidRequestHandler)
	http.ListenAndServe(address, nil)
}
