package main

import (
	//"encoding/json"
	"sort"
	"log"
	"time"
	//"bytes"

	eos "github.com/eoscanada/eos-go"
)

func main() {
	read_config()
	read_jwt()
	analyse()
}

func collect_all_fees(all *AllPhase) {
	for _, poolfee := range all.PoolFees {
		for i := 0; i < len(poolfee.Fees); i++ {
			found := false
			for j := 0; j < len(all.Fees); j++ {
				if poolfee.Fees[i].Symbol == all.Fees[j].Symbol {
					found = true
					all.Fees[j].Amount += poolfee.Fees[i].Amount
					break
				}
			}
			if !found {
				all.Fees = append(all.Fees, poolfee.Fees[i])
			}
		}
	}
}

func analyse() {
	phase1 := Phase{1, 0.8, 147745475, 149480609}
	phase2 := Phase{2, 0.7, 149480609, 152215761}
	phase3 := Phase{3, 0.7, 152215761, 152516868}
	phase4 := Phase{4, 0.7, 152516868, 152560201}
	all := &AllPhase{}
	all.PoolFees = PoolFeeList{}
	all.Days = []*Day{}
	all.Users = make(map[eos.AccountName]*User)
	analyse_phase(all, &phase1)
	analyse_phase(all, &phase2)
	analyse_phase(all, &phase3)
	analyse_phase(all, &phase4)
	sort.Stable(all.PoolFees)
	collect_all_fees(all)

	/*data, er := json.Marshal(all)
    if er != nil {
		log.Println("json.Marshal", er)
		return
	}
	var str bytes.Buffer
	_ = json.Indent(&str, data, "", "    ")
	file := "fees.json"
	write_file(file, str.String())*/

	save_totalfees(all.Fees)
	save_poolfees(all.PoolFees)
	save_userfees(all.Fees, all.Users)
	save_daypoolfees(all.Days)
	save_daypooluserfees(all.Days)
}

func analyse_phase(all *AllPhase, phase *Phase) {
	starttime := time.Now().Unix()
	log.Println("start analyse phase", phase.Id)
	for block := phase.BeginBlockNum; block < phase.EndBlockNum; {
		block = analyse_day(all, phase, block, phase.EndBlockNum)
	}
	stoptime := time.Now().Unix()
	log.Println("end analyse phase", phase.Id)
	log.Println("analysed", phase.EndBlockNum - phase.BeginBlockNum + 1, 
		"blocks, span", stoptime - starttime, "seconds")
}

func analyse_day(all *AllPhase, phase *Phase, begin_block_num uint64, end_block_num uint64) uint64 {
	_, updt := get_pools(begin_block_num)
	begin := get_swap_stats(begin_block_num)
	t := time.Unix(int64(updt), 0)
	day_end_block_num := begin_block_num + 172800//1 day
	if day_end_block_num > end_block_num {
		day_end_block_num = end_block_num
	}
	end := get_swap_stats(day_end_block_num)
	day := &Day{}
	day.Id = 1
	if len(all.Days) > 0 {
		day.Id = all.Days[len(all.Days) - 1].Id + 1
	}
	day.BeginBlockNum = begin_block_num
	day.EndBlockNum = day_end_block_num
	day.Pools = make(map[uint64]*DayPool)
	all.Days = append(all.Days, day)
	log.Println("analysing day", day.Id, t.Format("2006-01-02"), "block range:", day.BeginBlockNum, day.EndBlockNum)
	analyse_day_lptoken(all, phase, day, 240)
	analyse_day_summary(all, phase, day, begin, end)
	log.Println("analysed day", day.Id, t.Format("2006-01-02"), "block range:", day.BeginBlockNum, day.EndBlockNum)
	return day.EndBlockNum
}

func analyse_day_lptoken(all *AllPhase, phase *Phase, day *Day, skip_block_count uint64) {
	for block := day.BeginBlockNum; block <= day.EndBlockNum; block += skip_block_count {
		pools, _ := get_pools(block)
		for i := 0; i < len(pools); i++ {
			if _, iswhite := white_poolids[pools[i].Id]; !iswhite {
				continue
			}
			pool, existPool := day.Pools[pools[i].Id]
			if !existPool {
				pool = &DayPool{}
				pool.Id = pools[i].Id
				pool.BeginBlockNum = day.BeginBlockNum
				pool.EndBlockNum = day.EndBlockNum
				pool.Users = make(map[eos.AccountName]*DayUser)
				day.Pools[pool.Id] = pool
			}
			lptokens := get_lptokens(pools[i].Code, block)
			if len(lptokens) == 0 {
				log.Println("analyse_day_lptoken get no lptokens from block", block, "poolid", pools[i].Id)
				continue
			}
			var total_lptoken uint64 = 0
			for j := 0; j < len(lptokens); j++ {
				lptoken := &lptokens[j]
				var token uint64 = to_uint64(lptoken.Token)
				total_lptoken += token
				user, exist := pool.Users[lptoken.Owner]
				if !exist {
					user = &DayUser{}
					user.Owner = lptoken.Owner
					user.LPToken = 0
					pool.Users[lptoken.Owner] = user
				}
				user.LPToken += token
			}
			poolTotalLPtoken := to_uint64(pools[i].TotalLPtoken)
			if total_lptoken != poolTotalLPtoken {
				log.Println("analyse_day_lptoken block", block, "pool", pools[i].Id, "lptoken", poolTotalLPtoken, "!=", total_lptoken)
			}
			pool.TotalLPtoken += total_lptoken
		}
	}
}

func analyse_day_summary(all *AllPhase, phase *Phase, day *Day, begin []SwapStat, end []SwapStat) {
	for i := 0; i < len(end); i++ {
		endst := end[i]
		id := endst.Mid
		if _, iswhite := white_poolids[id]; !iswhite {
			continue
		}
		var poolfee *PoolFee = nil
		for x := 0; x < len(all.PoolFees); x++ {
			if all.PoolFees[x].Id == id {
				poolfee = all.PoolFees[x]
				break
			}
		}
		if poolfee == nil {
			poolfee = &PoolFee{}
			poolfee.Id = id
			all.PoolFees = append(all.PoolFees, poolfee)
		}
		pool, existPool := day.Pools[id]
		if !existPool {
			log.Println("analyse_day_summary no pool", id)
			continue
		}
		var beginst *SwapStat = nil
		if begin != nil {
			for j := 0; j < len(begin); j++ {
				if begin[j].Mid == id {
					beginst = &begin[j]
					break
				}
			}
		}
		if beginst == nil {
			for k := 0; k < len(endst.Stats); k++ {
				deltafee := endst.Stats[k].Fee
				deltafee.Amount = eos.Int64(float64(deltafee.Amount) * phase.FeeRatio)
				pool.Fees = append(pool.Fees, deltafee)
				found := false
				for j := 0; j < len(poolfee.Fees); j++ {
					if poolfee.Fees[j].Symbol == deltafee.Symbol {
						poolfee.Fees[j].Amount += deltafee.Amount
						found = true
						break
					}
				}
				if !found {
					poolfee.Fees = append(poolfee.Fees, deltafee)
				}
			}
		} else {
			for k := 0; k < len(endst.Stats); k++ {
				deltafee := endst.Stats[k].Fee
				if len(beginst.Stats) > k {
					deltafee.Amount -= beginst.Stats[k].Fee.Amount
				}
				deltafee.Amount = eos.Int64(float64(deltafee.Amount) * phase.FeeRatio)
				pool.Fees = append(pool.Fees, deltafee)
				found := false
				for j := 0; j < len(poolfee.Fees); j++ {
					if poolfee.Fees[j].Symbol == deltafee.Symbol {
						poolfee.Fees[j].Amount += deltafee.Amount
						found = true
						break
					}
				}
				if !found {
					poolfee.Fees = append(poolfee.Fees, deltafee)
				}
			}
		}

		for owner, user := range pool.Users {
			pool.Users[owner].LPRatio = float64(user.LPToken) / float64(pool.TotalLPtoken)
			for x := 0; x < len(pool.Fees); x++ {
				fee := pool.Fees[x]
				damount := user.LPRatio * float64(pool.Fees[x].Amount)
				uamount := uint64(damount * 1000000)
				fee.Amount = eos.Int64(damount)
				pool.Users[owner].UFees = append(pool.Users[owner].UFees, uamount)
				pool.Users[owner].Fees = append(pool.Users[owner].Fees, fee)

				if u, exist := all.Users[owner]; exist {
					found := false
					for y := 0; y < len(u.Fees); y++ {
						if u.Fees[y].Symbol == fee.Symbol {
							found = true
							u.UFees[y] += uamount
							u.Fees[y].Amount = eos.Int64(u.UFees[y] / 1000000)
							break
						}
					}
					if !found {
						u.UFees = append(u.UFees, uamount)
						u.Fees = append(u.Fees, fee)
					}
				} else {
					u = &User{}
					u.Owner = owner
					u.UFees = append(u.UFees, uamount)
					u.Fees = append(u.Fees, fee)
					all.Users[owner] = u
				}
			}
		}
	}
}
