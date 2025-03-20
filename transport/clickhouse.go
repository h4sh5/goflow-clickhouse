package transport

import (
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
)

var (
	ClickHouseAddr     *string
	ClickHousePort     *int
	ClickHouseUser     *string
	ClickHousePassword *string
	ClickHouseDatabase *string
	dbConn             *sql.DB
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
	log.Printf("[INFO] Connecting to ClickHouse at %s:%d\n", *ClickHouseAddr, *ClickHousePort)

	connStr := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&debug=false",
		*ClickHouseAddr, *ClickHousePort, *ClickHouseUser, *ClickHousePassword, *ClickHouseDatabase)

	connect, err := sql.Open("clickhouse", connStr)
	if err != nil {
		logger.Fatalf("[ERROR] Error connecting to ClickHouse: %v", err)
	}
	dbConn = connect

	// Test connection
	if err := dbConn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[ERROR] ClickHouse Error [%d]: %s\n", exception.Code, exception.Message)
		} else {
			log.Println(err)
		}
	}

	// **CREATE DATABASE (IF NOT EXISTS)**
	_, err = dbConn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *ClickHouseDatabase))
	if err != nil {
		logger.Fatalf("[ERROR] Could not create database '%s': %v", *ClickHouseDatabase, err)
	}

	// **CREATE TABLE WITH OPTIMIZATIONS**
	_, err = dbConn.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.nflow2 (
    TimeReceived DateTime,  -- ✅ No TTL on primary key
    TimeFlowStart DateTime,
    TimeFlowEnd DateTime TTL TimeFlowEnd + INTERVAL 3 MONTH,  -- ✅ Use TTL on TimeFlowEnd instead
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
ORDER BY (TimeReceived, SrcAddr, DstAddr, SrcPort, DstPort)  -- ✅ Fix ORDER BY
SETTINGS index_granularity = 8192;
`, *ClickHouseDatabase))

	if err != nil {
		logger.Fatalf("[ERROR] Could not create table: %v", err)
	}

	// **OPTIMIZE TABLE (AUTO-INDEXING)**
	_, err = dbConn.Exec(fmt.Sprintf("OPTIMIZE TABLE %s.nflow2 FINAL", *ClickHouseDatabase))
	if err != nil {
		logger.Printf("[WARN] Could not optimize table: %v", err)
	}

	log.Println("[INFO] ClickHouse connection established successfully.")
	return &ClickHouseState{FixedLengthProto: true}, nil
}

// Convert IPv4 address to UInt32 (BigEndian format)
func ipv4ToUInt32(ip net.IP) uint32 {
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

func ClickHouseInsert(fm *flowmessage.FlowMessage, stmt *sql.Stmt, wg *sync.WaitGroup) {
	// Convert IPv4 to UInt32
	srcAddr := ipv4ToUInt32(fm.GetSrcAddr())
	dstAddr := ipv4ToUInt32(fm.GetDstAddr())
	samplerAddr := ipv4ToUInt32(fm.GetSamplerAddress())

	// Logging for debugging
	log.Printf("[INFO] Flow Inserting: SrcAddr=%s, DstAddr=%s, SamplerAddress=%s, Bytes=%d, Packets=%d, Proto=%d\n",
		net.IP(fm.GetSrcAddr()), net.IP(fm.GetDstAddr()), net.IP(fm.GetSamplerAddress()),
		fm.GetBytes(), fm.GetPackets(), fm.GetProto())

	if _, err := stmt.Exec(
		time.Unix(int64(fm.GetTimeReceived()), 0),  // Convert to DateTime
		time.Unix(int64(fm.GetTimeFlowStart()), 0), // Convert to DateTime
		time.Unix(int64(fm.GetTimeFlowEnd()), 0),   // Convert to DateTime
		uint32(fm.GetBytes()),
		uint32(fm.GetEtype()),
		uint64(fm.GetPackets()),
		srcAddr,
		dstAddr,
		uint16(fm.GetSrcPort()),
		uint16(fm.GetDstPort()),
		uint8(fm.GetProto()),
		uint64(fm.GetSrcMac()),
		uint64(fm.GetDstMac()),
		uint16(fm.GetSrcVlan()),
		uint16(fm.GetDstVlan()),
		uint16(fm.GetVlanId()),
		uint8(fm.GetType()),
		samplerAddr,
	); err != nil {
		log.Printf("[ERROR] Error inserting record: %v\n", err)
	}

	wg.Done()
}

func (s ClickHouseState) Publish(msgs []*flowmessage.FlowMessage) {
	var wg sync.WaitGroup

	tx, err := dbConn.Begin()
	if err != nil {
		log.Printf("[ERROR] Transaction error: %v\n", err)
		return
	}
	defer tx.Commit()

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s.nflow2 (TimeReceived, TimeFlowStart, TimeFlowEnd, Bytes, Etype, Packets, SrcAddr, DstAddr, SrcPort, DstPort, Proto, SrcMac, DstMac, SrcVlan, DstVlan, VlanId, FlowType, SamplerAddress) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, *ClickHouseDatabase))

	if err != nil {
		log.Printf("[ERROR] Statement preparation error: %v\n", err)
		return
	}
	defer stmt.Close()

	for _, msg := range msgs {
		wg.Add(1)
		go ClickHouseInsert(msg, stmt, &wg)
	}

	wg.Wait()
	log.Println("[INFO] Batch insert completed.")
}
