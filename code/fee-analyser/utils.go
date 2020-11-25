package main

import (
	"encoding/json"
	"fmt"
	"os"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	"strconv"
	"reflect"

	eosws "github.com/dfuse-io/eosws-go"
	eos "github.com/eoscanada/eos-go"
)

type Config struct {
	Log				uint32 		`json:"log"`
	APIKey			string		`json:"api_key"`
	Node			string		`json:"node"`
}

type SwapStatInfo struct {
	Fee eos.Asset 			`json:"fee"`
	TradingVolume eos.Asset `json:"trading_volume"`
}

type SwapStat struct {
	Mid uint64				`json:"mid"`
	LastUpdateTime uint32 	`json:"last_update_time"`
	Stats []SwapStatInfo 	`json:"stats"`
}

type SwapStatDBRow struct {
	Payer	eos.AccountName	`json:"payer"`
	Row		SwapStat        `json:"json"`
}

type SwapStatStateResponse struct {
	Rows	[]SwapStatDBRow	`json:"rows"`
}

type LPtoken struct {
	Owner	eos.AccountName	`json:"owner"`
	Token 	interface{}		`json:"token"`
}

type LPtokenDBRow struct {
	Payer	eos.AccountName	`json:"payer"`
	Row		LPtoken         `json:"json"`
}

type LPtokenStateResponse struct {
	Rows	[]LPtokenDBRow	`json:"rows"`
}

type Pool struct {
	Id uint64					`json:"id"`
	Code string					`json:"code"`
	TotalLPtoken interface{}	`json:"total_lptoken"`
	LastUpdateTime uint32 		`json:"last_update_time"`
}

type PoolDBRow struct {
	Payer	eos.AccountName	`json:"payer"`
	Row		Pool	        `json:"json"`
}

type PoolStateResponse struct {
	Rows	[]PoolDBRow		`json:"rows"`
}

type DayUser struct {
	Owner		eos.AccountName	`json:"owner"`
	LPToken		uint64			`json:"lptoken"`
	LPRatio		float64			`json:"lpratio"`
	UFees 		[]uint64		`json:"ufees"`
	Fees 		[]eos.Asset		`json:"fees"`
}

type DayPool struct {
	Id 				uint64			`json:"id"`
	Fees 			[]eos.Asset		`json:"fees"`
	BeginBlockNum	uint64			`json:"begin_block_num"`
	EndBlockNum		uint64			`json:"end_block_num"`
	TotalLPtoken 	uint64			`json:"total_lptoken"`
	Users 			map[eos.AccountName]*DayUser		`json:"users"`
}

type Day struct {
	Id 				uint64			`json:"id"`
	BeginBlockNum	uint64			`json:"begin_block_num"`
	EndBlockNum		uint64			`json:"end_block_num"`
	Pools 			map[uint64]*DayPool		`json:"pools"`
}

type User struct {
	Owner		eos.AccountName	`json:"owner"`
	UFees 		[]uint64		`json:"ufees"`
	Fees 		[]eos.Asset		`json:"fees"`
}

type Phase struct {
	Id				uint64			`json:"id"`
	FeeRatio		float64			`json:"fee_ratio"`
	BeginBlockNum	uint64			`json:"begin_block_num"`
	EndBlockNum		uint64			`json:"end_block_num"`
}

type PoolFee struct {
	Id 				uint64			`json:"id"`
	Fees 			[]eos.Asset		`json:"fees"`
}

type PoolFeeList []*PoolFee

type AllPhase struct {
	Fees 			[]eos.Asset		`json:"fees"`
	PoolFees 		PoolFeeList		`json:"poolfees"`
	Days 			[]*Day			`json:"days"`
	Users 			map[eos.AccountName]*User	`json:"users"`
}

func (s PoolFeeList) Len() int { return len(s) }
func (s PoolFeeList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s PoolFeeList) Less(i, j int) bool { return s[i].Id < s[j].Id }

var config Config
var g_JWT string
var white_poolids map[uint64]uint64 = map[uint64]uint64{
	1:0,
	2:0,
	3:0,
	4:0,
	5:0,
	6:0,
	7:0,
	24:0,
	48:0,
	62:0,
	66:0,
	102:0,
}

func to_uint64(value interface{}) uint64 {
    switch value.(type) {
    case string:
        i64, err := strconv.ParseInt(value.(string), 10, 64)
        if err != nil {
            panic(err)
        }
        return (uint64)(i64)
    case uint64:
        return uint64(value.(uint64))
    case int64:
        return uint64(value.(int64))
    case uint32:
        return uint64(value.(uint32))
    case int32:
        return uint64(value.(int32))
    case uint16:
        return uint64(value.(uint16))
    case int16:
        return uint64(value.(int16))
    case float64:
        return uint64(value.(float64))
    default:
        log.Println("to_uint64 unknown type ", value, reflect.TypeOf(value))
        return 0
    }
}

func read_config() {
	data, err := ioutil.ReadFile("./config.json")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		panic(err)
	}
	if config.Log == 1 {
		log.Printf("config: %+v\n", config)
	}
}

func read_jwt() {
	file := "./jwt.txt"
	data, err := ioutil.ReadFile(file)
	if err == nil {
		g_JWT = string(data)
	}
	if len(g_JWT) == 0 {
		get_jwt(config.APIKey)
		write_file(file, g_JWT)
	}
	if config.Log == 1 {
		log.Printf("g_JWT: %+v\n", g_JWT)
	}
}

func write_file(fileName string, content string) error {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
	   fmt.Println("file create failed. err: " + err.Error())
	} else {
	   n, _ := f.Seek(0, os.SEEK_END)
	   _, err = f.WriteAt([]byte(content), n)
		if err != nil {
			fmt.Println("file WriteAt failed. err: " + err.Error())
		}
	   defer f.Close()
	}
	return err
}

func save_totalfees(fees []eos.Asset) {
	content := ""
	title := "fee\n"
	content += title
	for i := 0; i < len(fees); i++ {
		line := fmt.Sprintf("%v\n", fees[i].String())
		content += line
	}
	write_file("./fees/total_fees.csv", content)
}

func save_poolfees(poolfees PoolFeeList) {
	content := ""
	title := "poolid,token1,token2\n"
	content += title
	for i := 0; i < len(poolfees); i++ {
		line := fmt.Sprintf("%v", poolfees[i].Id)
		for j := 0; j < len(poolfees[i].Fees); j++ {
			line += fmt.Sprintf(",%v", poolfees[i].Fees[j].String())
		}
		line += "\n"
		content += line
	}
	write_file("./fees/pool_fees.csv", content)
}

func save_userfees(allfees []eos.Asset, users map[eos.AccountName]*User)  {
	for k := 0; k < len(allfees); k++ {
		symbol := allfees[k].Symbol
		content := ""
		title := "owner,token\n"
		content += title
		for _, v := range users {
			index := -1
			for i := 0; i < len(v.Fees); i++ {
				if v.Fees[i].Symbol == symbol && v.Fees[i].Amount > 0 {
					index = i
					break
				}
			}
			if index == -1 {
				continue
			}
			line := fmt.Sprintf("%v,%v\n", v.Owner, v.Fees[index].String())
			content += line
		}
		filename := fmt.Sprintf("./fees/user_%v_fees.csv", symbol.Symbol)
		write_file(filename, content)
	}
}

func save_daypoolfees(days []*Day) {
	for id, _ := range white_poolids {
		content := ""
		title := "day,beginblock,endblock,total_lptoken,token1,token2\n"
		content += title
		day_count := 0
		for i := 0; i < len(days); i++ {
			day := days[i]
			dayPool, exist := day.Pools[id]
			if exist {
				day_count++
				line := fmt.Sprintf("%v,%v,%v,%v", day.Id, day.BeginBlockNum, day.EndBlockNum, dayPool.TotalLPtoken)
				for k := 0; k < len(dayPool.Fees); k++ {
					line += fmt.Sprintf(",%v", dayPool.Fees[k].String())
				}
				line += "\n"
				content += line
			}
		}
		if day_count > 0 {
			filename := fmt.Sprintf("./fees/pool%v_dayfees.csv", id)
			write_file(filename, content)
		}
	}
}

func save_daypooluserfees(days []*Day) {
	for i := 0; i < len(days); i++ {
		day := days[i]
		for _, dayPool := range day.Pools {
			content := ""
			title := "owner,lptoken,lpratio,amount1,amount2,token1,token2\n"
			content += title
			for _, v := range dayPool.Users {
				line := fmt.Sprintf("%v,%v,%v", v.Owner, v.LPToken, v.LPRatio)
				for k := 0; k < len(v.UFees); k++ {
					line += fmt.Sprintf(",%v", v.UFees[k])
				}
				if len(v.UFees) == 1 {
					line += fmt.Sprintf(",%v", 0)
				}
				for k := 0; k < len(v.Fees); k++ {
					line += fmt.Sprintf(",%v", v.Fees[k].String())
				}
				if len(v.Fees) == 1 {
					line += ","
				}
				line += "\n"
				content += line
			}
			filename := fmt.Sprintf("./fees/pool%v_day%v_userfees.csv", dayPool.Id, i+1)
			write_file(filename, content)
		}
	}
}

func get_jwt(key string) {
	var jwt string
	var err error
	for {
		jwt, _, err = eosws.Auth(key)
		if err != nil {
			log.Printf("cannot get JWT token: %s", err.Error())
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}
	log.Println("Got new JWT:", jwt)
	g_JWT = jwt
}

func get_table(contract string, scope string, table string, block_num uint64, retryTimes int) []byte {
	retry := 0
	url := fmt.Sprintf("%v/v0/state/table?account=%v&scope=%v&table=%v&block_num=%v&json=true", 
			config.Node, contract, scope, table, block_num)
	for retry < retryTimes {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Println("http.NewRequest", url, "error:", err)
			retry += 1
			time.Sleep(time.Millisecond*500)
			continue
		}
		req.Header.Set("Authorization", "Bearer " + g_JWT)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("http.DefaultClient.Do", url, "error:", err)
			retry += 1
			time.Sleep(time.Millisecond*500)
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1048576))
		if err != nil {
			log.Println("ioutil.ReadAll", url, "error:", err)
			retry += 1
			time.Sleep(time.Millisecond*500)
			continue
		}
		if config.Log == 1 {
			log.Println("get_table", url, string(body))
		}
		return body
	}
	log.Println("get_table retry", retry, "failed, url", url)
	return []byte{}
}

func get_pools(block_num uint64) ([]Pool, uint32) {
	retry := 0
	for retry < 10 {
		a := []Pool{}
		var latest_update_time uint32 = 0
		bytes := get_table("dolphinsswap", "dolphinsswap", "pools", block_num, 10)
		rsp := &PoolStateResponse{}
		if err := json.Unmarshal(bytes, &rsp); err != nil {
			retry += 1
			log.Println("get_pool block_num", block_num, "Unmarshal", string(bytes), err, "try", retry)
			continue
		}
		for i := 0; i < len(rsp.Rows); i++ {
			if rsp.Rows[i].Row.LastUpdateTime > latest_update_time {
				latest_update_time = rsp.Rows[i].Row.LastUpdateTime
			}
			a = append(a, rsp.Rows[i].Row)
		}
		if len(a) == 0 {
			retry += 1
			log.Println("get_pool block_num", block_num, "no pools, try", retry)
			continue
		}
		if retry > 0 {
			log.Println("get_pool block_num", block_num, "got", len(a), "pools")
		}
		return a, latest_update_time
	}
	log.Println("get_pool block_num", block_num, "failed")
	return []Pool{}, 0
}

func get_lptokens(lpcode string, block_num uint64) []LPtoken {
	retry := 0
	for retry < 10 {
		a := []LPtoken{}
		bytes := get_table("dolphinsswap", lpcode, "lptokens", block_num, 10)
		rsp := &LPtokenStateResponse{}
		if err := json.Unmarshal(bytes, &rsp); err != nil {
			retry += 1
			log.Println("get_lptoken block_num", block_num, "Unmarshal", string(bytes), err, "try", retry)
			continue
		}
		for i := 0; i < len(rsp.Rows); i++ {
			a = append(a, rsp.Rows[i].Row)
		}
		if retry > 0 {
			log.Println("get_lptoken block_num", block_num, "got", len(a), "lptokens")
		}
		return a
	}
	return []LPtoken{}
}

func get_swap_stats(block_num uint64) []SwapStat {
	retry := 0
	for retry < 10 {
		a := []SwapStat{}
		bytes := get_table("dolphswaplog", "dolphswaplog", "swapstats", block_num, 10)
		rsp := &SwapStatStateResponse{}
		if err := json.Unmarshal(bytes, rsp); err != nil {
			retry += 1
			log.Println("get_swap_stat block_num", block_num, "Unmarshal", string(bytes), err, "try", retry)
			continue
		}
		for i := 0; i < len(rsp.Rows); i++ {
			a = append(a, rsp.Rows[i].Row)
		}
		if block_num >= 147747830 && len(a) == 0 {
			retry += 1
			log.Println("get_swap_stat block_num", block_num, "no swap_stats, try", retry)
			continue
		}
		if retry > 0 {
			log.Println("get_swap_stat block_num", block_num, "got", len(a), "swap_stats")
		}
		return a
	}
	return []SwapStat{}
}
