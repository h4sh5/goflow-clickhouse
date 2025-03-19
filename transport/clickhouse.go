package transport

import (
	"database/sql"
	"flag"
	"fmt"
	"sync"
	"time"
    "net"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
	"github.com/ClickHouse/clickhouse-go"
)

var (
	ClickHouseAddr     *string
	ClickHousePort     *int
	ClickHouseUser     *string
	ClickHousePassword *string
	ClickHouseDatabase *string
	dbConn             *sql.DB
	count              uint64
)

type ClickHouseState struct {
	FixedLengthProto bool
}

func RegisterFlags() {
	ClickHouseAddr = flag.String("ch.addr", "127.0.0.1", "ClickHouse DB Host")
	ClickHousePort = flag.Int("ch.port", 9000, "ClickHouse DB port")
	ClickHouseUser = flag.String("ch.username", "default", "ClickHouse username")
	ClickHousePassword = flag.String("ch.password", "default", "ClickHouse password")
	ClickHouseDatabase = flag.String("ch.database", "default", "ClickHouse database")
}

func StartClickHouseConnection(logger utils.Logger) (*ClickHouseState, error) {
	fmt.Printf("Connecting to ClickHouse at %s:%d\n", *ClickHouseAddr, *ClickHousePort)

	connStr := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&debug=false",
		*ClickHouseAddr, *ClickHousePort, *ClickHouseUser, *ClickHousePassword, *ClickHouseDatabase)

	connect, err := sql.Open("clickhouse", connStr)
	if err != nil {
		logger.Fatalf("Error connecting to ClickHouse: %v", err)
	}
	dbConn = connect

	// Test connection
	if err := dbConn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("ClickHouse Error [%d]: %s\n", exception.Code, exception.Message)
		} else {
			fmt.Println(err)
		}
	}

	// Create database if not exists
	_, err = dbConn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *ClickHouseDatabase))
	if err != nil {
		logger.Fatalf("Could not create database '%s': %v", *ClickHouseDatabase, err)
	}

	_, err = dbConn.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.nflow2 (
		TimeReceived DateTime,
		TimeFlowStart DateTime,
		TimeFlowEnd DateTime,
		Bytes UInt32,
		Etype UInt32,
		Packets UInt64,
		SrcAddr UInt32,
		DstAddr UInt32,
		SrcPort UInt16,
		DstPort UInt16,
		Proto UInt8,
		SrcMac UInt64,
		DstMac UInt64,
		SrcVlan UInt16,
		DstVlan UInt16,
		VlanId UInt16,
		FlowType UInt8,
		SamplerAddress UInt32
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMM(TimeReceived)
	ORDER BY (TimeReceived, SrcAddr, SrcPort, DstAddr, DstPort)
	SETTINGS index_granularity = 8192;
`, *ClickHouseDatabase))

	if err != nil {
		logger.Fatalf("Could not create table: %v", err)
	}

	return &ClickHouseState{FixedLengthProto: true}, nil
}
func ipv4ToUInt32(ip net.IP) uint32 {
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}
func ClickHouseInsert(fm *flowmessage.FlowMessage, stmt *sql.Stmt, wg *sync.WaitGroup) {
	// Convert IPv4 to UInt32
	srcAddr := ipv4ToUInt32(fm.GetSrcAddr())
	dstAddr := ipv4ToUInt32(fm.GetDstAddr())
	samplerAddr := ipv4ToUInt32(fm.GetSamplerAddress()) // ✅ Convert SamplerAddress

	if _, err := stmt.Exec(
		time.Unix(int64(fm.GetTimeReceived()), 0),   // ✅ Convert to DateTime
		time.Unix(int64(fm.GetTimeFlowStart()), 0),  // ✅ Convert to DateTime
		time.Unix(int64(fm.GetTimeFlowEnd()), 0),    // ✅ Convert to DateTime
		uint32(fm.GetBytes()),       // ✅ Ensure UInt32
		uint32(fm.GetEtype()),       // ✅ Ensure UInt32
		uint64(fm.GetPackets()),     // ✅ Ensure UInt64
		srcAddr,                     // ✅ Ensure UInt32 (Converted from IP)
		dstAddr,                     // ✅ Ensure UInt32 (Converted from IP)
		uint16(fm.GetSrcPort()),     // ✅ Ensure UInt16
		uint16(fm.GetDstPort()),     // ✅ Ensure UInt16
		uint8(fm.GetProto()),        // ✅ Ensure UInt8
		uint64(fm.GetSrcMac()),      // ✅ Ensure UInt64
		uint64(fm.GetDstMac()),      // ✅ Ensure UInt64
		uint16(fm.GetSrcVlan()),     // ✅ Ensure UInt16
		uint16(fm.GetDstVlan()),     // ✅ Ensure UInt16
		uint16(fm.GetVlanId()),      // ✅ Ensure UInt16
		uint8(fm.GetType()),         // ✅ Ensure UInt8
		samplerAddr,                 // ✅ Store as UInt32
	); err != nil {
		fmt.Printf("Error inserting record: %v\n", err)
	}

	wg.Done()
}


func (s ClickHouseState) Publish(msgs []*flowmessage.FlowMessage) {
	var wg sync.WaitGroup

	tx, err := dbConn.Begin()
	if err != nil {
		fmt.Printf("Transaction error: %v\n", err)
		return
	}
	defer tx.Commit()

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s.nflow2 (TimeReceived, TimeFlowStart, TimeFlowEnd, Bytes, Etype, Packets, SrcAddr, DstAddr, SrcPort, DstPort, Proto, SrcMac, DstMac, SrcVlan, DstVlan, VlanId, FlowType, SamplerAddress) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, *ClickHouseDatabase))

	if err != nil {
		fmt.Printf("Statement preparation error: %v\n", err)
		return
	}
	defer stmt.Close()

	for _, msg := range msgs {
		wg.Add(1)
		go ClickHouseInsert(msg, stmt, &wg)
	}

	wg.Wait()
}

