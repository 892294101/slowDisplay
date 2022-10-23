package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type ColInfo struct {
	TableName     string
	StatementType string
	ColumnNames   []string
	OrderColumns  []string
	millis        int32
}

type ColInfoSet struct {
	Cs map[string]*ColInfo
}

type Table struct {
	TableName     string
	StatementType string
	ColumnNames   string
	OrderColumns  string
}

type Elapsed struct {
	MinMillis int32
	MaxMillis int32
	Count     int
}

type StatisticsSet struct {
	cs map[Table]*Elapsed
}

func (s *StatisticsSet) Set(c *ColInfo) {
	tab := Table{TableName: c.TableName, StatementType: c.StatementType, ColumnNames: SliceToString(c.ColumnNames, ","), OrderColumns: SliceToString(c.OrderColumns, ",")}
	v, ok := s.cs[tab]
	if ok {
		if c.millis > v.MaxMillis {
			v.MaxMillis = c.millis
		}
		if c.millis < v.MinMillis {
			v.MinMillis = c.millis
		}
		v.Count += 1
	} else {
		s.cs[tab] = &Elapsed{MinMillis: c.millis, MaxMillis: c.millis, Count: 1}
	}
}

func (c *ColInfoSet) SetTableId(v string) {
	c.Cs[v] = &ColInfo{}
}

func (c *ColInfoSet) SetTableName(vid string, t string) {
	c.Cs[vid].TableName = t
}

func (c *ColInfoSet) SetColumn(vid string, t string, v []string, st string, s []string, millis int32) {

	c.Cs[vid].TableName = t

	if len(v) > 0 {
		c.Cs[vid].ColumnNames = append(c.Cs[vid].ColumnNames, v...)
	}
	if len(s) > 0 {
		c.Cs[vid].OrderColumns = append(c.Cs[vid].OrderColumns, s...)
	}

	c.Cs[vid].millis = millis
	c.Cs[vid].StatementType = st
}

func main() {
	var host = flag.String("host", "127.0.0.1:30000", "Enter host address")
	var db = flag.String("db", "", "Enter database name")
	var userName = flag.String("user", "", "Enter user and password.For example, user/pass")
	var table = flag.String("table", "", "Enter table name")
	var tm = flag.String("time", "", "Enter Slow log time")

	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var hp string

	if host != nil && len(*host) > 0 {
		hp = fmt.Sprintf("%s", *host)
	}

	if db == nil || len(*db) == 0 {
		fmt.Fprintf(os.Stderr, "%v\n", "Please enter the database name")
		return
	}

	if userName != nil && len(*userName) > 0 {
		hp = fmt.Sprintf("%s@hp", *userName)
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(fmt.Sprintf("mongodb://%v", hp)))

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	ctx, cancel = context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	collection := client.Database(*db).Collection("system.profile")

	err = client.Ping(ctx, readpref.Primary())

	var timezone = time.FixedZone("CST", 8*3600)
	filter := bson.D{}
	if (table != nil && len(*table) > 0) && (tm != nil && len(*tm) > 0) {
		//filter = bson.D{{"pop", bson.D{{"$gte", 500}}}}

		t, err := time.ParseInLocation("2006-01-02 15:04:05", *tm, timezone)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}

		filter = bson.D{
			{"ns", fmt.Sprintf("%v.%v", *db, *table)},
			{"ts", bson.D{{"$gte", primitive.NewDateTimeFromTime(t)}}},
		}
	}

	if (table != nil && len(*table) > 0) && len(*tm) == 0 {
		filter = bson.D{
			{"ns", fmt.Sprintf("%v.%v", *db, *table)},
		}
	}

	if (table == nil || len(*table) == 0) && (tm != nil && len(*tm) > 0) {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", *tm, timezone)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}
		filter = bson.D{
			{"ts", bson.D{{"$gte", primitive.NewDateTimeFromTime(t)}}},
		}
	}

	cur, err := collection.Find(ctx, filter)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Find: %s\n", err.Error())
		return
	}
	defer cur.Close(ctx)
	var cs ColInfoSet
	cs.Cs = make(map[string]*ColInfo)
	for cur.Next(ctx) {
		var result bson.D
		err := cur.Decode(&result)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}

		d := result.Map()

		id, _ := uuid.GenerateUUID()

		vid, ok := d["_id"]
		if ok {
			cs.SetTableId(fmt.Sprintf("%v", vid))
		} else {
			cs.SetTableId(fmt.Sprintf("%v", id))
			vid = id
		}

		vtb, _ := d["ns"]
		cs.SetTableName(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb))
		var millis int32
		ms, ok := d["millis"]
		if ok {
			switch m := ms.(type) {
			case int32:
				millis = m
			}
		}

		v, ok := d["query"]
		if ok {
			switch ds := v.(type) {
			case primitive.D:
				q := ds.Map()
				// 获取排序列
				var sft []string
				vq, ok := q["sort"]
				if ok {
					switch vqv := vq.(type) {
					case primitive.D:
						for _, e := range vqv {
							sft = append(sft, fmt.Sprintf("%v:%v", e.Key, e.Value))
						}

					}
				}

				// 获取操作类型
				vop, _ := d["op"]

				// 获取过滤列
				var fcol []string
				vq, ok = q["filter"]
				if ok {
					switch vqv := vq.(type) {
					case primitive.D:
						for _, e := range vqv {
							fcol = append(fcol, fmt.Sprintf("%v", e.Key))
						}

					}

				}

				col := []string{} // 列
				var ops string    // 操作类型

				if fcol != nil {
					col = fcol
				}
				if vop != nil {
					ops = fmt.Sprintf("%v", vop)
				}

				softCol := []string{}
				if sft != nil {
					softCol = sft
				}

				cs.SetColumn(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb), col, fmt.Sprintf("%v", ops), softCol, millis)

				//cs.SetColumn(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb), fcol, fmt.Sprintf("%v", vop), sft, millis)

			}

		}

		v, ok = d["command"]
		if ok {
			switch ds := v.(type) {
			case primitive.D:
				q := ds.Map()
				// 获取命令类型
				var ccou string
				_, ok := q["count"]
				if ok {
					ccou = "count"

					// 获取count 使用的列 条件
					var fcol []string
					v, ok := q["query"]
					if ok {
						switch ds := v.(type) {
						case primitive.D:
							for _, e := range ds {
								fcol = append(fcol, fmt.Sprintf("%v", e.Key))
							}
						}
					}

					cs.SetColumn(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb), fcol, fmt.Sprintf("%v", ccou), []string{}, millis)

				}

				_, ok = q["findandmodify"]
				if ok {
					ccou = "findandmodify"
					// 获取count 使用的列 条件
					var fcol []string
					v, ok := q["query"]
					if ok {
						switch ds := v.(type) {
						case primitive.D:
							for _, e := range ds {
								fcol = append(fcol, fmt.Sprintf("%v", e.Key))
							}
						}
					}

					cs.SetColumn(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb), fcol, fmt.Sprintf("%v", ccou), []string{}, millis)
				}

				_, ok = q["aggregate"]
				if ok {
					ccou = "aggregate"
					// 获取count 使用的列 条件
					var fcol []string
					var sft []string
					v, ok := q["pipeline"]
					if ok {
						switch ds := v.(type) {
						case primitive.A:
							for _, i2 := range ds {
								switch ds2 := i2.(type) {
								case primitive.D:
									ds3 := ds2.Map()
									dsv, ok := ds3["$group"]
									if ok {
										switch ids := dsv.(type) {
										case primitive.D:
											for _, i := range ids.Map() {
												switch is := i.(type) {
												case primitive.D:
													for _, i3 := range is.Map() {
														switch i3d := i3.(type) {
														case string:
															ind := strings.Index(i3d, ".")
															if ind != -1 {
																fcol = append(fcol, i3d[ind+1:])
															}

														}
													}
												}
											}
										}
									}

									vq, ok := ds3["$sort"]
									if ok {
										switch vqv := vq.(type) {
										case primitive.D:
											for _, e := range vqv {
												if e.Value == 1 || e.Value == -1 {
													sft = append(sft, fmt.Sprintf("%v:%v", e.Key, e.Value))
												}

											}

										}
									}

								}
							}
						}
					}

					cs.SetColumn(fmt.Sprintf("%v", vid), fmt.Sprintf("%v", vtb), fcol, fmt.Sprintf("%v", ccou), sft, millis)
				}

			}
		}

	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	// 所有slow 日志条目
	/*for _, info := range cs.Cs {
		fmt.Printf("Table: %-35s Type: %-15s Column: %-50s Order: %-30s millis: %v\n",
			info.TableName,
			info.StatementType,
			SliceToString(info.ColumnNames, ","),
			SliceToString(info.OrderColumns, ","),
			info.millis,
		)
	}*/

	tabwide := 5
	Typewide := 5
	Columnwide := 2
	Orderwide := 2

	var st StatisticsSet
	st.cs = make(map[Table]*Elapsed)
	for _, info := range cs.Cs {
		if len(info.TableName) > tabwide-4 {
			tabwide = len(info.TableName) + 4
		}

		if len(info.StatementType) > Typewide-4 {
			Typewide = len(info.StatementType) + 4
		}

		cn := SliceToString(info.ColumnNames, ",")
		if len(cn) > Columnwide-4 {
			Columnwide = len(cn) + 4
		}
		oc := SliceToString(info.OrderColumns, ",")
		if len(oc) > Orderwide-4 {
			Orderwide = len(oc) + 4
		}

		st.Set(info)
	}

	ind := make(map[string]*string)
	x := Sorts(&st)
	if x != nil && len(x.res) > 0 {
		fmt.Printf("\n\nThe slow summary statistics are as follows:\n")
		for i, table := range x.res {
			head := "Table: %-" + strconv.Itoa(tabwide) + "s Type: %-" + strconv.Itoa(Typewide) + "s Column: %-" + strconv.Itoa(Columnwide) + "s Order: %-" + strconv.Itoa(Orderwide) + "s MinMillis: %-8d MaxMillis: %-8d Count: %-4d\n"
			fmt.Printf(head, table.TableName, table.StatementType, table.ColumnNames, table.OrderColumns, x.elm[i].MinMillis, x.elm[i].MaxMillis, x.elm[i].Count)
			ind[table.TableName] = nil
		}
	}

	if len(ind) > 0 {
		fmt.Printf("\n\nThe index summary statistics are as follows:\n")
		for tab, _ := range ind {
			index, err := GetIndex(client, tab)
			if err != nil {
				fmt.Printf("get index error: %v\n", err.Error())
				return
			}
			if len(*index) > 0 {
				fmt.Printf("+++++++ Table: %v\n%v\n", tab, *index)
			}
		}
	}

}

func GetIndex(client *mongo.Client, tab string) (*string, error) {
	var o, t string
	li := strings.Index(tab, ".")
	if li != -1 {
		o, t = tab[:li], tab[li+1:]
	}

	collection := client.Database(o).Collection(t)
	cur, err := collection.Indexes().List(context.Background(), options.ListIndexes().SetBatchSize(2))
	if err != nil {
		return nil, err
	}

	var result []bson.M
	if err := cur.All(context.Background(), &result); err != nil {
		return nil, err
	}

	var buf strings.Builder
	if len(result) > 0 {
		for _, v := range result {
			for k1, v1 := range v {
				if !strings.EqualFold(k1, "v") && !strings.EqualFold(k1, "ns") {
					buf.WriteString(fmt.Sprintf("%v: %v ", k1, v1))
				}
			}
			buf.WriteString(fmt.Sprintf("\n"))
		}
		x := buf.String()
		return &x, nil
	}
	x := ""
	return &x, nil
}

type softSlow struct {
	res []Table
	elm []*Elapsed
}

func Sorts(data *StatisticsSet) *softSlow {
	var list []int32
	ss := new(softSlow)
	n := len(data.cs)
	didSwap := false
	for table, elapsed := range data.cs {
		list = append(list, elapsed.MaxMillis)
		ss.res = append(ss.res, table)
		ss.elm = append(ss.elm, elapsed)
	}

	if list != nil && ss.res != nil && ss.elm != nil {
		// 进行 N-1 轮迭代
		for i := n - 1; i > 0; i-- {
			// 每次从第一位开始比较，比较到第 i 位就不比较了，因为前一轮该位已经有序了
			for j := 0; j < i; j++ {
				// 如果前面的数比后面的大，那么交换
				if list[j] > list[j+1] {
					list[j], list[j+1] = list[j+1], list[j]
					ss.res[j], ss.res[j+1] = ss.res[j+1], ss.res[j]
					ss.elm[j], ss.elm[j+1] = ss.elm[j+1], ss.elm[j]
					didSwap = true
				}
			}
			// 如果在一轮中没有交换过，那么已经排好序了，直接返回
			// [1,2,3,4,5]，每一次比较，前面的数都比后面的数小
			if !didSwap {
				break
			}
		}

	}
	return ss
}

//切片转为字符类型
func SliceToString(kv []string, sp string) string {
	var kwsb strings.Builder
	var kw string
	if len(kv) > 0 {
		for i, v := range kv {
			if i == 0 {
				kwsb.WriteString(v)
			} else {
				if len(sp) > 0 {
					kwsb.WriteString(sp)
				} else {
					kwsb.WriteString(" ")
				}
				kwsb.WriteString(v)

			}
		}
		kw = kwsb.String()
		return kw
	}
	return ""
}
