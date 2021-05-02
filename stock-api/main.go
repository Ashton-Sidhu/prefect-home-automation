package main

import "C"

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// APIKEY ... Alpha Vantage API key, stored as env variable
var APIKEY string = os.Getenv("ALPHA_API_KEY")

// ENDPOINT ... Alpha Vantage API daily stock data endpoint
var ENDPOINT string = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"

// Data ... Data structure
type Data struct {
	MetaData   MetaData               `json:"Meta Data"`
	TimeSeries map[string]interface{} `json:"Time Series (Daily)"`
}

// MetaData ... Stock metadata structure
type MetaData struct {
	Info        string `json:"1. Information"`
	Symbol      string `json:"2. Symbol"`
	LastRefresh string `json:"3. Last Refreshed"`
	OutputSize  string `json:"4. Output Size"`
	TimeZone    string `json:"5. Time Zone"`
}

// FinData ... Daily Financial Data json structure
type FinData struct {
	Open       string `json:"1. open"`
	High       string `json:"2. high"`
	Low        string `json:"3. low"`
	Close      string `json:"4. close"`
	AdjClose   string `json:"5. adjusted close"`
	Volume     string `json:"6. volume"`
	DivAmount  string `json:"7. dividend amount"`
	SplitCoeff string `json:"8. split coefficient"`
}

func main() {}

//export getPrice
func getPrice(ticker *C.char, date *C.char) *C.char {

	tickerDate := C.GoString(date)
	stock := C.GoString(ticker)

	query := fmt.Sprintf("%s&symbol=%s&apikey=%s", ENDPOINT, stock, APIKEY)

	client := http.Client{
		Timeout: time.Second * 10, // Timeout after 5 seconds
	}

	req, err := http.NewRequest(http.MethodGet, query, nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("User-Agent", "stock-api-project")

	resp, getErr := client.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)

	dailyData := Data{}
	json.Unmarshal(respBody, &dailyData)

	// Encode Interface as bytes
	dailyFinDataMap := dailyData.TimeSeries[tickerDate]
	dfdByte, _ := json.Marshal(dailyFinDataMap)

	// Map interface to FinData struct
	dailyFinData := FinData{}
	json.Unmarshal(dfdByte, &dailyFinData)

	return C.CString(dailyFinData.AdjClose)
}
